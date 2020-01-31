

pub mod protocol {
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Command {
        Set(u32, String),
        Get(u32),
    }
}