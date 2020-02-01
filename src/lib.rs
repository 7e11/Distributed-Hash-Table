

pub mod protocol {
    use serde::{Serialize, Deserialize};

    pub type KeyType = i32;
    pub type ValueType = String;

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Command {
        Put(KeyType, ValueType),
        Get(KeyType),
    }

}