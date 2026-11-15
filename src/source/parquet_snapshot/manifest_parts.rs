use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error, ser::SerializeSeq};
use uuid::Uuid;

use super::{MAX_DELETED, Part};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPart<'a> {
    id: Uuid,
    rows: usize,
    #[serde(default, skip_serializing_if = "<[usize]>::is_empty")]
    deleted: Cow<'a, [usize]>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deleted_ranges: Vec<[usize; 2]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_range: Option<[i64; 2]>,
}

pub(super) fn serialize<S: Serializer>(parts: &[Part], serializer: S) -> Result<S::Ok, S::Error> {
    let mut sequence = serializer.serialize_seq(Some(parts.len()))?;
    for part in parts {
        let limit = part.deleted.len() / 3;
        let mut ranges = part
            .deleted
            .chunk_by(|left, right| left + 1 == *right)
            .take(limit + 1)
            .map(|run| [run[0], run[run.len() - 1] + 1])
            .collect::<Vec<_>>();
        // Each range stores two offsets. Retain sparse masks as positions.
        if ranges.len() > limit {
            ranges.clear();
        }
        sequence.serialize_element(&StoredPart {
            id: part.id,
            rows: part.rows,
            deleted: if ranges.is_empty() {
                Cow::Borrowed(&part.deleted)
            } else {
                Cow::Borrowed(&[])
            },
            deleted_ranges: ranges,
            key_range: part.key_range,
        })?;
    }
    sequence.end()
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<Part>, D::Error> {
    let mut remaining = MAX_DELETED;
    Vec::<StoredPart<'_>>::deserialize(deserializer)?
        .into_iter()
        .map(|stored| {
            let mut deleted = stored.deleted.into_owned();
            if !deleted.is_empty() && !stored.deleted_ranges.is_empty() {
                return Err(D::Error::custom(
                    "local Parquet part has two deletion masks",
                ));
            }
            remaining = remaining
                .checked_sub(deleted.len())
                .ok_or_else(|| D::Error::custom("local Parquet deletion budget exceeded"))?;
            let mut previous_end = 0;
            for [start, end] in stored.deleted_ranges {
                if start < previous_end || end <= start || end > stored.rows {
                    return Err(D::Error::custom("invalid local Parquet deletion range"));
                }
                remaining = remaining
                    .checked_sub(end - start)
                    .ok_or_else(|| D::Error::custom("local Parquet deletion budget exceeded"))?;
                deleted.extend(start..end);
                previous_end = end;
            }
            Ok(Part {
                id: stored.id,
                rows: stored.rows,
                deleted,
                key_range: stored.key_range,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Serialize, Deserialize)]
    struct Parts(#[serde(with = "super")] Vec<Part>);

    #[test]
    fn dense_and_sparse_masks_round_trip_in_the_same_manifest() -> anyhow::Result<()> {
        let mut parts = Parts(vec![Part {
            id: Uuid::new_v4(),
            rows: 32,
            deleted: vec![0, 1, 2, 29, 30, 31],
            key_range: None,
        }]);
        parts.0.push(Part {
            id: Uuid::new_v4(),
            deleted: vec![1, 8, 29],
            ..parts.0[0].clone()
        });
        let encoded = serde_json::to_value(&parts)?;
        assert_eq!(encoded[0]["deleted_ranges"], json!([[0, 3], [29, 32]]));
        assert_eq!(encoded[1]["deleted"], json!([1, 8, 29]));
        let decoded: Parts = serde_json::from_value(encoded)?;
        assert_eq!(decoded.0[0].deleted, parts.0[0].deleted);
        assert_eq!(decoded.0[1].deleted, parts.0[1].deleted);
        Ok(())
    }

    #[test]
    fn ranges_cannot_expand_past_the_snapshot_budget() {
        fn part(ranges: &[[usize; 2]]) -> serde_json::Value {
            json!({"id": Uuid::new_v4(), "rows": 2 * MAX_DELETED, "deleted_ranges": ranges})
        }
        let invalid = [
            json!([part(&[[0, 4], [3, 5]])]),
            json!([part(&[[0, 2 * MAX_DELETED + 1]])]),
            json!([part(&[[0, MAX_DELETED]]), part(&[[7, 8]])]),
        ];
        for encoded in invalid {
            assert!(serde_json::from_value::<Parts>(encoded).is_err());
        }
    }
}
