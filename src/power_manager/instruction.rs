use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum MaxRampPower {
    Cumulative(f32),
    PerComponent(HashMap<u64, f32>),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Instruction {
    ProposePower {
        component_ids: Vec<u64>,
        power: f32,
        priority: i64,
        max_ramp_power: Option<MaxRampPower>,
    },
}
