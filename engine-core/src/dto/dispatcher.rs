use actix::Message;

use crate::dto::tadpole::Tadpole;

#[derive(Message)]
#[rtype(result = "Tadpole")]
pub struct Dispatch {
    pub tadpole: Tadpole,
}
