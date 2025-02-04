use arrow_flight::{
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{record_batches_to_flight_stream, to_tonic_err, Service},
    timing::{TimeMeasurement, TimedStream},
};

/// Get a `FlightInfo` for listing catalogs.
pub(crate) fn get_flight_info(
    query: &sql::CommandGetCatalogs,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    tracing::trace!("get_flight_info_catalogs");
    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd);

    Response::new(info)
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandGetCatalogs,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = TimeMeasurement::new("flight_do_get_get_catalogs_duration_ms", vec![]);
    tracing::trace!("do_get_catalogs: {query:?}");
    let mut builder = query.into_builder();

    let catalog_names = flight_svc.datafusion.read().await.ctx.catalog_names();

    for catalog in catalog_names {
        builder.append(catalog);
    }

    let record_batch = builder.build().map_err(to_tonic_err)?;

    Ok(Response::new(Box::pin(TimedStream::new(
        record_batches_to_flight_stream(vec![record_batch]),
        move || start,
    ))
        as <Service as FlightService>::DoGetStream))
}
