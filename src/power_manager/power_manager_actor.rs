use super::instruction::Instruction;

pub(super) struct PowerManagerActor {
    client: crate::MicrogridClientHandle,
    instructions_rx: tokio::sync::mpsc::Receiver<Instruction>,
}

impl PowerManagerActor {
    pub fn new(
        client: crate::MicrogridClientHandle,
        instructions_rx: tokio::sync::mpsc::Receiver<Instruction>,
    ) -> Self {
        Self {
            client,
            instructions_rx,
        }
    }

    pub async fn run(mut self) {
        while let Some(instruction) = self.instructions_rx.recv().await {
            match instruction {
                Instruction::ProposePower {
                    component_ids,
                    power,
                    priority,
                    max_ramp_power,
                } => {}
            }
        }
    }
}
