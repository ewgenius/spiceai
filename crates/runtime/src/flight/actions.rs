use std::fmt::{self, Display, Formatter};

use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{flightsql::prepared_statement_query, to_tonic_err, Service},
    timing::{TimeMeasurement, TimedStream},
};

use arrow_flight::{
    flight_service_server::FlightService,
    sql::{self, Any, ProstMessageExt},
    Action, ActionType as FlightActionType,
};

enum ActionType {
    CreatePreparedStatement,
    ClosePreparedStatement,
    Unknown,
}

impl ActionType {
    fn from_str(s: &str) -> Self {
        match s {
            "CreatePreparedStatement" => ActionType::CreatePreparedStatement,
            "ClosePreparedStatement" => ActionType::ClosePreparedStatement,
            _ => ActionType::Unknown,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            ActionType::CreatePreparedStatement => "CreatePreparedStatement",
            ActionType::ClosePreparedStatement => "ClosePreparedStatement",
            ActionType::Unknown => "Unknown",
        }
    }
}

impl Display for ActionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub(crate) fn list() -> Response<<Service as FlightService>::ListActionsStream> {
    tracing::trace!("list_actions");
    let create_prepared_statement_action_type = FlightActionType {
        r#type: ActionType::CreatePreparedStatement.to_string(),
        description: "Creates a reusable prepared statement resource on the server.\n
            Request Message: ActionCreatePreparedStatementRequest\n
            Response Message: ActionCreatePreparedStatementResult"
            .into(),
    };
    let close_prepared_statement_action_type = FlightActionType {
        r#type: ActionType::ClosePreparedStatement.to_string(),
        description: "Closes a reusable prepared statement resource on the server.\n
            Request Message: ActionClosePreparedStatementRequest\n
            Response Message: N/A"
            .into(),
    };
    let actions: Vec<Result<FlightActionType, Status>> = vec![
        Ok(create_prepared_statement_action_type),
        Ok(close_prepared_statement_action_type),
    ];

    let output = TimedStream::new(futures::stream::iter(actions), || {
        TimeMeasurement::new("flight_list_actions_duration_ms", vec![])
    });

    Response::new(Box::pin(output) as <Service as FlightService>::ListActionsStream)
}

pub(crate) async fn do_action(
    flight_svc: &Service,
    request: Request<Action>,
) -> Result<Response<<Service as FlightService>::DoActionStream>, Status> {
    let action_type = ActionType::from_str(request.get_ref().r#type.as_str());

    let action_type_str = action_type.as_str().to_string();
    let start = TimeMeasurement::new(
        "flight_do_action_duration_ms",
        vec![("action_type", action_type_str)],
    );

    let stream = match action_type {
        ActionType::CreatePreparedStatement => {
            tracing::trace!("do_action: CreatePreparedStatement");
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionCreatePreparedStatementRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedStatementRequest.",
                    )
                })?;
            let stmt =
                prepared_statement_query::do_action_create_prepared_statement(flight_svc, cmd)
                    .await?;
            futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })])
        }
        ActionType::ClosePreparedStatement => {
            tracing::trace!("do_action: ClosePreparedStatement");
            futures::stream::iter(vec![Ok(arrow_flight::Result::default())])
        }
        ActionType::Unknown => return Err(Status::invalid_argument("Unknown action type")),
    };

    Ok(Response::new(Box::pin(TimedStream::new(
        stream,
        move || start,
    ))))
}
