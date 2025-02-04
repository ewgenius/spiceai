use std::sync::Arc;

use arrow_flight::{flight_service_server::FlightService, FlightData, SchemaAsIpc};
use arrow_ipc::writer::{self, DictionaryTracker, IpcDataGenerator};
use futures::{stream, StreamExt};
use tokio::sync::broadcast;
use tonic::{Request, Response, Status, Streaming};

use crate::dataupdate::{DataUpdate, UpdateType};

use super::Service;

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoExchangeStream>, Status> {
    let mut streaming_request = request.into_inner();
    let req = streaming_request.next().await;
    let Some(subscription_request) = req else {
        return Err(Status::invalid_argument(
            "Need to send a FlightData message with a FlightDescriptor to subscribe to",
        ));
    };

    let subscription_request = match subscription_request {
        Ok(subscription_request) => subscription_request,
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "Unable to read subscription request: {e}",
            )));
        }
    };

    // TODO: Support multiple flight descriptors to subscribe to multiple data sources
    let Some(flight_descriptor) = subscription_request.flight_descriptor else {
        return Err(Status::invalid_argument(
            "Flight descriptor required to indicate which data to subscribe to",
        ));
    };

    if flight_descriptor.path.is_empty() {
        return Err(Status::invalid_argument(
            "Flight descriptor needs to specify a path to indicate which data to subscribe to",
        ));
    };

    let data_path = flight_descriptor.path.join(".");

    if !flight_svc
        .datafusion
        .read()
        .await
        .has_publishers(&data_path)
    {
        return Err(Status::invalid_argument(format!(
            r#"Unknown dataset: "{data_path}""#,
        )));
    };

    let channel_map = Arc::clone(&flight_svc.channel_map);
    let channel_map_read = channel_map.read().await;
    let (tx, rx) = if let Some(channel) = channel_map_read.get(&data_path) {
        (Arc::clone(channel), channel.subscribe())
    } else {
        drop(channel_map_read);
        let mut channel_map_write = channel_map.write().await;
        let (tx, rx) = broadcast::channel(100);
        let tx = Arc::new(tx);
        channel_map_write.insert(data_path.clone(), Arc::clone(&tx));
        (tx, rx)
    };

    let response_stream = stream::unfold(rx, move |mut rx| {
        let encoder = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let write_options = writer::IpcWriteOptions::default();
        async move {
            match rx.recv().await {
                Ok(data_update) => {
                    let mut schema_sent: bool = false;

                    let mut flights = vec![];

                    for batch in &data_update.data {
                        if !schema_sent {
                            let schema = batch.schema();
                            flights
                                .push(FlightData::from(SchemaAsIpc::new(&schema, &write_options)));
                            schema_sent = true;
                        }
                        let Ok((flight_dictionaries, flight_batch)) =
                            encoder.encoded_batch(batch, &mut tracker, &write_options)
                        else {
                            panic!("Unable to encode batch")
                        };

                        flights.extend(flight_dictionaries.into_iter().map(Into::into));
                        flights.push(flight_batch.into());
                    }

                    metrics::counter!("flight_do_exchange_data_updates_sent")
                        .increment(flights.len() as u64);
                    let output = futures::stream::iter(flights.into_iter().map(Ok));

                    Some((output, rx))
                }
                Err(_e) => {
                    let output = futures::stream::iter(vec![].into_iter().map(Ok));
                    Some((output, rx))
                }
            }
        }
    })
    .flat_map(|x| x);

    let datafusion = Arc::clone(&flight_svc.datafusion);
    tokio::spawn(async move {
        let Ok(df) = datafusion
            .read()
            .await
            .ctx
            .sql(&format!(r#"SELECT * FROM "{data_path}""#))
            .await
        else {
            return;
        };
        let Ok(results) = df.collect().await else {
            return;
        };
        if results.is_empty() {
            return;
        }

        for batch in &results {
            let data_update = DataUpdate {
                data: vec![batch.clone()],
                update_type: UpdateType::Append,
            };
            let _ = tx.send(data_update);
        }
    });

    Ok(Response::new(response_stream.boxed()))
}
