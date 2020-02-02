

pub mod protocol {
    use serde::{Serialize, Deserialize};

    pub type KeyType = i32;         // TODO: Make generic (from config file?)
    pub type ValueType = String;    // TODO: Should be Any (Or equivalent)

    #[derive(Serialize, Deserialize, Debug)]
    pub enum Command {
        Put(KeyType, ValueType),
        Get(KeyType),
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub enum BarrierCommand {
        AllReady,
        NotAllReady,
    }

}