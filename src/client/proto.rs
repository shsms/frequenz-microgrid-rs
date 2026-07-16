// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Generated protobuf modules for the Frequenz API.

// Only export what we need
pub use frequenz_api_microgrid::common::v1alpha8 as common;
#[cfg(any(test, feature = "test-utils"))]
pub use frequenz_api_microgrid::google;
pub use frequenz_api_microgrid::microgrid::v1alpha18 as microgrid;

mod graph;
pub use graph::{GraphComponent, GraphConnection};
