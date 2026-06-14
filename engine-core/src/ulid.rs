use std::sync::Mutex;

use log::debug;
use ulid::{Generator, Ulid};

#[derive(Default)]
pub struct UlidService {
    generator: Mutex<Generator>,
}

impl UlidService {
    pub fn new() -> Self {
        Self {
            generator: Mutex::new(Generator::new()),
        }
    }

    pub fn generate(&self) -> Ulid {
        let mut generator = self.generator.lock().expect("ULID generator poisoned");
        let ulid = generator
            .generate()
            .expect("ULID generation should not fail");
        debug!("ulid generated: {}", ulid);
        ulid
    }

    pub fn generate_string(&self) -> String {
        self.generate().to_string()
    }
}
