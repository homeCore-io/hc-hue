use std::collections::HashMap;

use super::models::{BridgeSnapshot, HueAuxDevice, HueGroupedLight, HueLight, HueScene};

#[derive(Debug, Clone)]
pub struct RegisteredLight {
    pub bridge_id: String,
    pub light_rid: String,
    pub supports_gradient: bool,
    pub supports_identify: bool,
    pub effect_values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RegisteredGroup {
    pub bridge_id: String,
    pub group_rid: String,
}

#[derive(Debug, Clone)]
pub struct RegisteredScene {
    pub bridge_id: String,
    pub scene_rid: String,
}

#[derive(Debug, Clone)]
pub struct RegisteredAux {
    pub bridge_id: String,
    pub resource_type: String,
    pub resource_rid: String,
}

#[derive(Debug, Default)]
pub struct HueRegistry {
    by_device_id: HashMap<String, BridgeSnapshot>,
    lights_by_device_id: HashMap<String, RegisteredLight>,
    groups_by_device_id: HashMap<String, RegisteredGroup>,
    scenes_by_device_id: HashMap<String, RegisteredScene>,
    aux_by_device_id: HashMap<String, RegisteredAux>,
}

impl HueRegistry {
    pub fn upsert_bridge(&mut self, device_id: String, snapshot: BridgeSnapshot) {
        self.by_device_id.insert(device_id, snapshot);
    }

    pub fn bridge_count(&self) -> usize {
        self.by_device_id.len()
    }

    pub fn online_count(&self) -> usize {
        self.by_device_id
            .values()
            .filter(|snapshot| snapshot.online)
            .count()
    }

    pub fn total_summary_bytes(&self) -> usize {
        self.by_device_id
            .values()
            .map(|snapshot| snapshot.summary.to_string().len())
            .sum()
    }

    pub fn ensure_light(&mut self, light: &HueLight) -> bool {
        let existed = self.lights_by_device_id.contains_key(&light.device_id);
        self.lights_by_device_id.insert(
            light.device_id.clone(),
            RegisteredLight {
                bridge_id: light.bridge_id.clone(),
                light_rid: light.resource_id.clone(),
                supports_gradient: light.supports_gradient,
                supports_identify: light.supports_identify,
                effect_values: light.effect_values.clone(),
            },
        );
        !existed
    }

    pub fn get_light_binding(&self, device_id: &str) -> Option<&RegisteredLight> {
        self.lights_by_device_id.get(device_id)
    }

    pub fn light_count(&self) -> usize {
        self.lights_by_device_id.len()
    }

    pub fn find_light_device_id(&self, bridge_id: &str, light_rid: &str) -> Option<String> {
        self.lights_by_device_id.iter().find_map(|(device_id, binding)| {
            if binding.bridge_id == bridge_id && binding.light_rid == light_rid {
                Some(device_id.clone())
            } else {
                None
            }
        })
    }

    pub fn ensure_group(&mut self, group: &HueGroupedLight) -> bool {
        if self.groups_by_device_id.contains_key(&group.device_id) {
            return false;
        }
        self.groups_by_device_id.insert(
            group.device_id.clone(),
            RegisteredGroup {
                bridge_id: group.bridge_id.clone(),
                group_rid: group.resource_id.clone(),
            },
        );
        true
    }

    pub fn get_group_binding(&self, device_id: &str) -> Option<&RegisteredGroup> {
        self.groups_by_device_id.get(device_id)
    }

    pub fn group_count(&self) -> usize {
        self.groups_by_device_id.len()
    }

    pub fn find_group_device_id(&self, bridge_id: &str, group_rid: &str) -> Option<String> {
        self.groups_by_device_id.iter().find_map(|(device_id, binding)| {
            if binding.bridge_id == bridge_id && binding.group_rid == group_rid {
                Some(device_id.clone())
            } else {
                None
            }
        })
    }

    pub fn ensure_scene(&mut self, scene: &HueScene) -> bool {
        if self.scenes_by_device_id.contains_key(&scene.device_id) {
            return false;
        }
        self.scenes_by_device_id.insert(
            scene.device_id.clone(),
            RegisteredScene {
                bridge_id: scene.bridge_id.clone(),
                scene_rid: scene.resource_id.clone(),
            },
        );
        true
    }

    pub fn get_scene_binding(&self, device_id: &str) -> Option<&RegisteredScene> {
        self.scenes_by_device_id.get(device_id)
    }

    pub fn scene_count(&self) -> usize {
        self.scenes_by_device_id.len()
    }

    pub fn find_scene_device_id(&self, bridge_id: &str, scene_rid: &str) -> Option<String> {
        self.scenes_by_device_id.iter().find_map(|(device_id, binding)| {
            if binding.bridge_id == bridge_id && binding.scene_rid == scene_rid {
                Some(device_id.clone())
            } else {
                None
            }
        })
    }

    pub fn ensure_aux(&mut self, aux: &HueAuxDevice) -> bool {
        if self.aux_by_device_id.contains_key(&aux.device_id) {
            return false;
        }
        self.aux_by_device_id.insert(
            aux.device_id.clone(),
            RegisteredAux {
                bridge_id: aux.bridge_id.clone(),
                resource_type: aux.resource_type.clone(),
                resource_rid: aux.resource_id.clone(),
            },
        );
        true
    }

    pub fn aux_count(&self) -> usize {
        self.aux_by_device_id.len()
    }

    pub fn get_aux_binding(&self, device_id: &str) -> Option<&RegisteredAux> {
        self.aux_by_device_id.get(device_id)
    }

    pub fn find_aux_device_id(
        &self,
        bridge_id: &str,
        resource_type: &str,
        resource_rid: &str,
    ) -> Option<String> {
        self.aux_by_device_id.iter().find_map(|(device_id, binding)| {
            if binding.bridge_id == bridge_id
                && binding.resource_type == resource_type
                && binding.resource_rid == resource_rid
            {
                Some(device_id.clone())
            } else {
                None
            }
        })
    }
}
