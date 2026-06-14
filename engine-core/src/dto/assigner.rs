use actix::Message;

use crate::dto::tadpole::Tadpole;

#[derive(Message)]
#[rtype(result = "Vec<Tadpole>")]
pub struct Assign {
    pub envelope: domain::envelope::CanonicalEnvelope,
}
