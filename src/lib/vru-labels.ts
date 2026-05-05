const VRU_DETECTION_LABELS = new Set([
  "person",
  "pedestrian",
  "child",
  "kids",
  "construction worker",
  "person wearing safety vest",
  "work-zone-person",
  "work zone person",
  "bicycle",
  "bicycle rider",
  "cyclist",
  "motorcycle",
  "motorcycle rider",
  "motorcyclist",
  "scooter",
  "scooter rider",
  "electric scooter",
  "electric kick scooter",
  "wheelchair",
  "stroller",
  "skateboard",
  "skateboarder",
  "animal",
  "cat",
  "dog",
  "deer",
  "bird",
  "horse",
  "sheep",
  "cow",
  "bear",
  "elephant",
  "zebra",
  "giraffe",
]);

export const VRU_OBJECT_FILTER_OPTIONS = [
  {
    value: "person",
    label: "Person",
    aliases: [
      "person",
      "pedestrian",
      "child",
      "kids",
      "construction worker",
      "person wearing safety vest",
      "work-zone-person",
      "work zone person",
    ],
  },
  {
    value: "stroller",
    label: "Stroller",
    aliases: ["stroller"],
  },
  {
    value: "scooter",
    label: "Scooter",
    aliases: ["scooter", "scooter rider", "electric scooter", "electric kick scooter"],
  },
  {
    value: "wheelchair",
    label: "Wheelchair",
    aliases: ["wheelchair"],
  },
  {
    value: "bicycle",
    label: "Bicycle",
    aliases: ["bicycle", "bicycle rider", "cyclist"],
  },
  {
    value: "motorcycle",
    label: "Motorcycle",
    aliases: ["motorcycle", "motorcycle rider", "motorcyclist"],
  },
  {
    value: "skateboard",
    label: "Skateboard",
    aliases: ["skateboard", "skateboarder"],
  },
  {
    value: "dog",
    label: "Dog",
    aliases: ["dog"],
  },
  {
    value: "animal",
    label: "Animal",
    aliases: ["animal", "cat", "dog", "deer", "bird", "horse", "sheep", "cow"],
  },
  {
    value: "car",
    label: "Car",
    aliases: ["car"],
  },
  {
    value: "truck",
    label: "Truck",
    aliases: ["truck"],
  },
  {
    value: "bus",
    label: "Bus",
    aliases: ["bus"],
  },
  {
    value: "crosswalk",
    label: "Crosswalk",
    aliases: ["crosswalk"],
  },
] as const;

export type VruObjectFilterValue =
  (typeof VRU_OBJECT_FILTER_OPTIONS)[number]["value"];

const VRU_OBJECT_FILTER_VALUE_SET = new Set<string>(
  VRU_OBJECT_FILTER_OPTIONS.map((option) => option.value)
);

export function normalizeDetectionLabel(label: string): string {
  return label.trim().toLowerCase().replace(/_/g, " ").replace(/\s+/g, " ");
}

export function isVruDetectionLabel(label: unknown): label is string {
  return typeof label === "string" && VRU_DETECTION_LABELS.has(normalizeDetectionLabel(label));
}

export function isVruObjectFilterValue(value: string): value is VruObjectFilterValue {
  return VRU_OBJECT_FILTER_VALUE_SET.has(value);
}

export function getVruObjectFilterLabel(value: string): string {
  return VRU_OBJECT_FILTER_OPTIONS.find((option) => option.value === value)?.label ?? value;
}

export function expandVruObjectFilterAliases(values: string[]): string[] {
  const aliases = new Set<string>();

  for (const value of values) {
    const normalized = normalizeDetectionLabel(value);
    const option = VRU_OBJECT_FILTER_OPTIONS.find((item) => item.value === normalized);
    if (option) {
      option.aliases.forEach((alias) => aliases.add(normalizeDetectionLabel(alias)));
    } else {
      aliases.add(normalized);
    }
  }

  return Array.from(aliases);
}
