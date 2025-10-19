use hyper::StatusCode;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OrlbError {
    #[error("invalid JSON-RPC payload: {0}")]
    InvalidPayload(String),
    #[error("mutating method `{0}` is not permitted")]
    MutatingMethod(String),
    #[error("no healthy providers available")]
    NoHealthyProviders,
    #[error("upstream request failed: {0}")]
    Upstream(String),
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: i32,
    pub message: String,
}

impl OrlbError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            OrlbError::InvalidPayload(_) => StatusCode::BAD_REQUEST,
            OrlbError::MutatingMethod(_) => StatusCode::FORBIDDEN,
            OrlbError::NoHealthyProviders => StatusCode::SERVICE_UNAVAILABLE,
            OrlbError::Upstream(_) => StatusCode::BAD_GATEWAY,
        }
    }

    pub fn to_body(&self) -> ErrorBody {
        let (code, message) = match self {
            OrlbError::InvalidPayload(msg) => (-32000, msg.clone()),
            OrlbError::MutatingMethod(method) => {
                (-32601, format!("method `{method}` is not allowed"))
            }
            OrlbError::NoHealthyProviders => (-32100, "no healthy providers available".into()),
            OrlbError::Upstream(msg) => (-32001, msg.clone()),
        };

        ErrorBody { code, message }
    }
}
