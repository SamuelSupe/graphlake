use crate::BusinessTask;

pub(super) struct Step {
    pub kind: &'static str,
    pub pattern: &'static str,
    pub fields: &'static str,
    pub evidence: &'static [(&'static str, &'static str)],
    pub snapshot: bool,
}

pub(super) fn anchor(task: BusinessTask) -> Step {
    match task {
        BusinessTask::OffboardedAccountExposure => Step {
            kind: "person",
            pattern: "MATCH (p:Person) WHERE p.namespace = $namespace AND p.person_id = $person_id",
            fields: "p.tenant_id AS tenant_id, p.departed_at_ms AS departed_at_ms",
            evidence: &[("p", "Person")],
            snapshot: false,
        },
        BusinessTask::AlertIdentityDeviceContext => Step {
            kind: "alert",
            pattern: "MATCH (n:Alert) WHERE n.alert_id = $alert_id",
            fields: "n.tenant_id AS tenant_id, n.happened_at_ms AS happened_at_ms",
            evidence: &[("n", "Alert")],
            snapshot: false,
        },
    }
}

pub(super) fn steps(task: BusinessTask) -> &'static [Step] {
    match task {
        BusinessTask::OffboardedAccountExposure => OFFBOARDING,
        BusinessTask::AlertIdentityDeviceContext => ALERT_CONTEXT,
    }
}

const OFFBOARDING: &[Step] = &[
    Step {
        kind: "residual_grant",
        pattern: "
            MATCH (p:Person)<-[i:IDENTIFIED_AS]-(a:Account)-[g:GRANTED]->(r:Resource)
            WHERE p.tenant_id = $tenant AND p.namespace = $namespace AND p.person_id = $person_id
              AND (i.valid_from_ms IS NULL OR i.valid_from_ms <= $as_of)
              AND (i.valid_to_ms IS NULL OR i.valid_to_ms > $as_of)
              AND (g.valid_from_ms IS NULL OR g.valid_from_ms <= $as_of)
              AND (g.valid_to_ms IS NULL OR g.valid_to_ms > $as_of)",
        fields: "a.account_id AS account_id, r.resource_id AS resource_id, g.action AS action",
        evidence: &[("i", "IDENTIFIED_AS"), ("g", "GRANTED")],
        snapshot: true,
    },
    Step {
        kind: "residual_role_grant",
        pattern: "
            MATCH (p:Person)<-[i:IDENTIFIED_AS]-(a:Account)-[m:MEMBER_OF]->(b:Role)-[g:ROLE_GRANT]->(r:Resource)
            WHERE p.tenant_id = $tenant AND p.namespace = $namespace AND p.person_id = $person_id
              AND (i.valid_from_ms IS NULL OR i.valid_from_ms <= $as_of)
              AND (i.valid_to_ms IS NULL OR i.valid_to_ms > $as_of)
              AND (m.valid_from_ms IS NULL OR m.valid_from_ms <= $as_of)
              AND (m.valid_to_ms IS NULL OR m.valid_to_ms > $as_of)
              AND (g.valid_from_ms IS NULL OR g.valid_from_ms <= $as_of)
              AND (g.valid_to_ms IS NULL OR g.valid_to_ms > $as_of)",
        fields: "a.account_id AS account_id, b.role_id AS role_id, r.resource_id AS resource_id, g.action AS action",
        evidence: &[("i", "IDENTIFIED_AS"), ("m", "MEMBER_OF"), ("g", "ROLE_GRANT")],
        snapshot: true,
    },
    Step {
        kind: "access_after_departure",
        pattern: "
            MATCH (p:Person)<-[i:IDENTIFIED_AS]-(a:Account)-[x:ACCESSED]->(r:Resource)
            WHERE p.tenant_id = $tenant AND p.namespace = $namespace AND p.person_id = $person_id
              AND x.happened_at_ms >= $departed
              AND x.happened_at_ms >= $window_start AND x.happened_at_ms < $window_end
              AND (i.valid_from_ms IS NULL OR i.valid_from_ms <= x.happened_at_ms)
              AND (i.valid_to_ms IS NULL OR i.valid_to_ms > x.happened_at_ms)",
        fields: "a.account_id AS account_id, r.resource_id AS resource_id, x.action AS action,
                 x.happened_at_ms AS happened_at_ms, x.outcome AS outcome",
        evidence: &[("i", "IDENTIFIED_AS"), ("x", "ACCESSED")],
        snapshot: false,
    },
];

const ALERT_CONTEXT: &[Step] = &[
    Step {
        kind: "direct_account",
        pattern: "MATCH (n:Alert)-[d:ALERT_ACCOUNT]->(a:Account)
                  WHERE n.tenant_id = $tenant AND n.alert_id = $alert_id",
        fields: "a.account_id AS account_id",
        evidence: &[("d", "ALERT_ACCOUNT")],
        snapshot: false,
    },
    Step {
        kind: "direct_device",
        pattern: "MATCH (n:Alert)-[d:ALERT_DEVICE]->(a:Device)
                  WHERE n.tenant_id = $tenant AND n.alert_id = $alert_id",
        fields: "a.device_id AS device_id",
        evidence: &[("d", "ALERT_DEVICE")],
        snapshot: false,
    },
    Step {
        kind: "identity_assignment",
        pattern: "MATCH (n:Alert)-[d:ALERT_ACCOUNT]->(a:Account)-[i:IDENTIFIED_AS]->(p:Person)
                  WHERE n.tenant_id = $tenant AND n.alert_id = $alert_id
                    AND (i.valid_from_ms IS NULL OR i.valid_from_ms <= $alert_time)
                    AND (i.valid_to_ms IS NULL OR i.valid_to_ms > $alert_time)",
        fields: "a.account_id AS account_id, p.namespace AS namespace, p.person_id AS person_id,
                 i.valid_from_ms AS valid_from_ms, i.valid_to_ms AS valid_to_ms",
        evidence: &[("d", "ALERT_ACCOUNT"), ("i", "IDENTIFIED_AS")],
        snapshot: false,
    },
    Step {
        kind: "device_correlation",
        pattern: "MATCH (n:Alert)-[d:ALERT_ACCOUNT]->(a:Account)-[x:USED_DEVICE]->(b:Device)
                  WHERE n.tenant_id = $tenant AND n.alert_id = $alert_id
                    AND x.happened_at_ms >= $window_start AND x.happened_at_ms < $window_end",
        fields: "a.account_id AS account_id, b.device_id AS device_id, x.happened_at_ms AS happened_at_ms",
        evidence: &[("d", "ALERT_ACCOUNT"), ("x", "USED_DEVICE")],
        snapshot: false,
    },
    Step {
        kind: "account_device_correlation",
        pattern: "MATCH (n:Alert)-[d:ALERT_DEVICE]->(b:Device)<-[x:USED_DEVICE]-(a:Account)
                  WHERE n.tenant_id = $tenant AND n.alert_id = $alert_id
                    AND x.happened_at_ms >= $window_start AND x.happened_at_ms < $window_end",
        fields: "a.account_id AS account_id, b.device_id AS device_id, x.happened_at_ms AS happened_at_ms",
        evidence: &[("d", "ALERT_DEVICE"), ("x", "USED_DEVICE")],
        snapshot: false,
    },
    Step {
        kind: "shared_ip_correlation",
        pattern: "MATCH (n:Alert)-[d:ALERT_ACCOUNT]->(a:Account)-[x:USED_IP]->(ip:IP)<-[y:USED_IP]-(b:Account)
                  WHERE n.tenant_id = $tenant AND n.alert_id = $alert_id AND a.account_id <> b.account_id
                    AND x.happened_at_ms >= $window_start AND x.happened_at_ms < $window_end
                    AND y.happened_at_ms >= $window_start AND y.happened_at_ms < $window_end",
        fields: "a.account_id AS account_id, b.account_id AS related_account_id, ip.address AS address,
                 x.happened_at_ms AS happened_at_ms, y.happened_at_ms AS related_at_ms",
        evidence: &[("d", "ALERT_ACCOUNT"), ("x", "USED_IP"), ("y", "USED_IP")],
        snapshot: false,
    },
];
