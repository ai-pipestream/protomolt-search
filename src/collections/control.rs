//! The cluster-control surface over one durable plane per collection.

use super::*;

/// The cluster-control surface over one durable plane per collection.
#[derive(Clone)]
pub struct ClusterControlSet {
    pub(super) members: Members<ClusterControlService>,
}

impl ClusterControlSet {
    pub fn single(control: ClusterControlService) -> Self {
        ClusterControlSet {
            members: Members::single(control),
        }
    }

    pub fn named(
        members: Vec<(String, ClusterControlService)>,
        default: Option<String>,
    ) -> Result<Self, String> {
        for (name, control) in &members {
            if control.collection() != *name {
                return Err(format!(
                    "collection {name:?}: its control plane was opened for {:?}",
                    control.collection()
                ));
            }
        }
        Ok(ClusterControlSet {
            members: Members::named(members, default, "control planes")?,
        })
    }

    pub fn resolve(&self, collection: &str) -> Result<&ClusterControlService, Status> {
        self.members.resolve(collection).map(|(_, member)| member)
    }

    pub fn members(&self) -> Vec<(&str, &ClusterControlService)> {
        let mut out: Vec<(&str, &ClusterControlService)> = self
            .members
            .named
            .iter()
            .map(|(name, member)| (name.as_str(), member))
            .collect();
        if let Some(member) = &self.members.unnamed {
            out.push(("", member));
        }
        out
    }

    pub fn into_server(self, max_message_bytes: usize) -> ClusterControlServer<Self> {
        ClusterControlServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}

macro_rules! cluster_control_over_collections {
    ($( $name:ident : $req:ty => $resp:ty ),* $(,)?) => {
        #[tonic::async_trait]
        impl ClusterControl for ClusterControlSet {
            $(
                async fn $name(
                    &self,
                    mut request: Request<$req>,
                ) -> Result<Response<$resp>, Status> {
                    let (name, target) = self.members.resolve(&request.get_ref().collection)?;
                    let target = target.clone();
                    request.get_mut().collection = name;
                    ClusterControl::$name(&target, request).await
                }
            )*
        }
    };
}

cluster_control_over_collections! {
    register_node: RegisterNodeRequest => NodeLease,
    renew_node_lease: RenewNodeLeaseRequest => NodeLease,
    drain_node: DrainNodeRequest => ClusterPlan,
    report_shard: ReportShardRequest => ClusterPlan,
    complete_placement_action: CompletePlacementActionRequest => ClusterPlan,
    reconcile_cluster: ReconcileClusterRequest => ClusterPlan,
    get_cluster_plan: GetClusterPlanRequest => ClusterPlan,
    rollback_cluster: RollbackClusterRequest => ClusterPlan,
    plan_balance: PlanBalanceRequest => PlanBalanceResponse,
}
