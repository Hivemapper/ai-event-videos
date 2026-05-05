export const VRU_PRIORITY_FIRMWARE_NUM = 7_004_003;
export const VRU_PRIORITY_FPS_QC_BUCKETS = ["perfect", "ok"] as const;

const bucketListSql = VRU_PRIORITY_FPS_QC_BUCKETS.map((bucket) => `'${bucket}'`).join(", ");

export function buildVruPriorityOrderBy(
  triageAlias: string,
  fpsQcAlias: string
): string {
  return `
    CASE
      WHEN ${triageAlias}.firmware_version_num >= ${VRU_PRIORITY_FIRMWARE_NUM}
       AND ${fpsQcAlias}.bucket IN (${bucketListSql})
      THEN 0 ELSE 1
    END ASC,
    CASE WHEN ${triageAlias}.firmware_version_num >= ${VRU_PRIORITY_FIRMWARE_NUM} THEN 0 ELSE 1 END ASC,
    CASE WHEN ${fpsQcAlias}.bucket IN (${bucketListSql}) THEN 0 ELSE 1 END ASC
  `;
}
