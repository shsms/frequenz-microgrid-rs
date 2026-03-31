use crate::{Error, MicrogridClientHandle};

use super::instruction::{Instruction, MaxRampPower};

pub struct PowerManagerHandle {
    instructions_tx: tokio::sync::mpsc::Sender<Instruction>,
}

impl PowerManagerHandle {
    pub fn new(client: MicrogridClientHandle) -> Self {
        let (instructions_tx, instructions_rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(
            super::power_manager_actor::PowerManagerActor::new(client, instructions_rx).run(),
        );
        Self { instructions_tx }
    }

    pub async fn propose_power(
        &self,
        component_ids: Vec<u64>,
        power: f32,
        priority: i64,
        max_ramp_power: Option<MaxRampPower>,
    ) -> Result<(), Error> {
        let instruction = Instruction::ProposePower {
            component_ids,
            power,
            priority,
            max_ramp_power,
        };

        self.instructions_tx
            .send(instruction)
            .await
            .map_err(|e| Error::internal(format!("Failed to send instruction: {e}")))
    }
}
