import { AIEventType } from "@/types/events";

export interface HighlightEvent {
  id: string;
  type: AIEventType;
  location: string;
  coords: { lat: number; lon: number };
  date: string;
  maxSpeed: number; // km/h
  minSpeed: number; // km/h
  acceleration: number; // m/s²
}

export interface HighlightSection {
  title: string;
  description: string;
  events: HighlightEvent[];
}

export const highlightSections: HighlightSection[] = [
  {
    title: "Extreme Braking",
    description:
      "Events with the largest speed drops — over 90 km/h of deceleration captured on dashcam.",
    events: [
      {
        id: "68693232d2b06edd1cd1ed9d",
        type: "HARSH_BRAKING",
        location: "Bailey County, TX, USA",
        coords: { lat: 34.3077, lon: -102.8774 },
        date: "Jul 05, 2025",
        maxSpeed: 123.8,
        minSpeed: 1.1,
        acceleration: 1.592,
      },
      {
        id: "6867ff149abbc70fa1f2e3ab",
        type: "HARSH_BRAKING",
        location: "Mooskirchen, Austria",
        coords: { lat: 46.9793, lon: 15.2831 },
        date: "Jul 04, 2025",
        maxSpeed: 147.9,
        minSpeed: 30.5,
        acceleration: 1.361,
      },
      {
        id: "69581dad62cb7e369e720878",
        type: "HARSH_BRAKING",
        location: "Camarillo, CA, USA",
        coords: { lat: 34.2168, lon: -119.0343 },
        date: "Jan 02, 2026",
        maxSpeed: 113.7,
        minSpeed: 5.1,
        acceleration: 1.442,
      },
      {
        id: "68bb0935716411932b9feb6d",
        type: "HARSH_BRAKING",
        location: "Randall County, TX, USA",
        coords: { lat: 35.0985, lon: -101.9138 },
        date: "Sep 05, 2025",
        maxSpeed: 105.6,
        minSpeed: 0.0,
        acceleration: 1.316,
      },
      {
        id: "690a7281957cb58b9d79a392",
        type: "HARSH_BRAKING",
        location: "Cleveland, TX, USA",
        coords: { lat: 30.3087, lon: -95.1088 },
        date: "Nov 04, 2025",
        maxSpeed: 107.9,
        minSpeed: 6.6,
        acceleration: 1.378,
      },
      {
        id: "6868045a770201fcdb88d2c1",
        type: "HARSH_BRAKING",
        location: "Miami-Dade County, FL, USA",
        coords: { lat: 25.9015, lon: -80.2101 },
        date: "Jul 04, 2025",
        maxSpeed: 98.7,
        minSpeed: 1.4,
        acceleration: 1.359,
      },
      {
        id: "686800901aae3deca26fd943",
        type: "HARSH_BRAKING",
        location: "Sankt Stefan ob Stainz, Austria",
        coords: { lat: 46.9333, lon: 15.2687 },
        date: "Jul 04, 2025",
        maxSpeed: 111.8,
        minSpeed: 15.3,
        acceleration: 1.288,
      },
      {
        id: "690a4d3d45b733d5692e1343",
        type: "HARSH_BRAKING",
        location: "Hillsborough County, FL, USA",
        coords: { lat: 28.0326, lon: -82.4925 },
        date: "Nov 04, 2025",
        maxSpeed: 94.0,
        minSpeed: 0.0,
        acceleration: 1.25,
      },
      {
        id: "68e093d3a52cff4f3d6e2cb1",
        type: "HARSH_BRAKING",
        location: "Dallas, TX, USA",
        coords: { lat: 32.759, lon: -96.8087 },
        date: "Oct 04, 2025",
        maxSpeed: 97.6,
        minSpeed: 5.2,
        acceleration: 1.334,
      },
      {
        id: "6892146f3085332227096dc6",
        type: "HARSH_BRAKING",
        location: "Wrocław, Poland",
        coords: { lat: 51.1161, lon: 17.0009 },
        date: "Aug 05, 2025",
        maxSpeed: 91.6,
        minSpeed: 0.0,
        acceleration: 1.279,
      },
      {
        id: "6976a302e13d2ed988573033",
        type: "HARSH_BRAKING",
        location: "Los Angeles, CA, USA",
        coords: { lat: 34.1230304, lon: -118.3411565 },
        date: "Jan 25, 2026",
        maxSpeed: 0,
        minSpeed: 0,
        acceleration: 1.422,
      },
      {
        id: "697c1b9ca308a518511d0900",
        type: "HARSH_BRAKING",
        location: "Los Angeles, CA, USA",
        coords: { lat: 34.0544884, lon: -118.2377482 },
        date: "Jan 30, 2026",
        maxSpeed: 0,
        minSpeed: 0,
        acceleration: 1.411,
      },
    ],
  },
  {
    title: "Highest G-Force",
    description:
      "The most intense acceleration events recorded — peak G-force moments caught on camera.",
    events: [
      {
        id: "6983ce656c7db1c683897f4f",
        type: "HIGH_G_FORCE",
        location: "New Orleans, LA, USA",
        coords: { lat: 29.9643523, lon: -90.09933 },
        date: "Feb 04, 2026",
        maxSpeed: 56.1,
        minSpeed: 24.7,
        acceleration: 1.859,
      },
      {
        id: "6983b8141f82e0fbece64d2c",
        type: "HIGH_G_FORCE",
        location: "New Orleans, LA, USA",
        coords: { lat: 30.0187886, lon: -90.0980387 },
        date: "Feb 04, 2026",
        maxSpeed: 45.9,
        minSpeed: 0.0,
        acceleration: 1.818,
      },
      {
        id: "6983d0418527fe88f4aa1925",
        type: "HIGH_G_FORCE",
        location: "Hillsborough County, FL, USA",
        coords: { lat: 28.003615, lon: -82.4916033 },
        date: "Feb 04, 2026",
        maxSpeed: 38.0,
        minSpeed: 7.1,
        acceleration: 1.775,
      },
      {
        id: "6983bf1453fd0ca157cdce8f",
        type: "HIGH_G_FORCE",
        location: "Coffee County, GA, USA",
        coords: { lat: 31.500841, lon: -82.7882975 },
        date: "Feb 04, 2026",
        maxSpeed: 86.6,
        minSpeed: 56.0,
        acceleration: 1.773,
      },
      {
        id: "6983d40dc45afe6380ef6adc",
        type: "HIGH_G_FORCE",
        location: "Faro, Portugal",
        coords: { lat: 37.0994559, lon: -7.9607886 },
        date: "Feb 04, 2026",
        maxSpeed: 40.1,
        minSpeed: 10.8,
        acceleration: 1.729,
      },
    ],
  },
  {
    title: "Swerving",
    description:
      "Sudden lane departures and evasive maneuvers caught on camera.",
    events: [
      {
        id: "69830f800c905a8a524d337e",
        type: "HARSH_BRAKING",
        location: "Gdynia, Poland",
        coords: { lat: 54.5520069, lon: 18.4218529 },
        date: "Feb 04, 2026",
        maxSpeed: 0,
        minSpeed: 0,
        acceleration: 1.241,
      },
    ],
  },
  {
    title: "International Highlights",
    description:
      "Notable events from around the world — a sample of driving incidents across different countries.",
    events: [
      {
        id: "6983dc9cedd6c6e0e651cf8a",
        type: "AGGRESSIVE_ACCELERATION",
        location: "Querétaro, Mexico",
        coords: { lat: 20.552439, lon: -100.3862985 },
        date: "Feb 04, 2026",
        maxSpeed: 46.6,
        minSpeed: 10.4,
        acceleration: 1.175,
      },
      {
        id: "6983dbe723811241a9714bbc",
        type: "HIGH_G_FORCE",
        location: "New South Wales, Australia",
        coords: { lat: -32.967605, lon: 151.5357565 },
        date: "Feb 04, 2026",
        maxSpeed: 102.3,
        minSpeed: 100.1,
        acceleration: 1.61,
      },
      {
        id: "6983d59f0c905a8a52a0511e",
        type: "HIGH_G_FORCE",
        location: "Kaohsiung, Taiwan",
        coords: { lat: 22.5851584, lon: 120.3277383 },
        date: "Feb 04, 2026",
        maxSpeed: 57.6,
        minSpeed: 40.2,
        acceleration: 1.517,
      },
      {
        id: "6983d40dc45afe6380ef6adc",
        type: "HIGH_G_FORCE",
        location: "Faro, Portugal",
        coords: { lat: 37.0994559, lon: -7.9607886 },
        date: "Feb 04, 2026",
        maxSpeed: 40.1,
        minSpeed: 10.8,
        acceleration: 1.729,
      },
      {
        id: "6983d1501f82e0fbec0b67ac",
        type: "AGGRESSIVE_ACCELERATION",
        location: "Cancún, Mexico",
        coords: { lat: 21.1641461, lon: -86.8798143 },
        date: "Feb 04, 2026",
        maxSpeed: 45.9,
        minSpeed: 10.2,
        acceleration: 1.213,
      },
      {
        id: "6983ced4445c4da1f1297cfe",
        type: "SWERVING",
        location: "Fremantle, Australia",
        coords: { lat: -32.0581313, lon: 115.7539242 },
        date: "Feb 04, 2026",
        maxSpeed: 40.6,
        minSpeed: 0.3,
        acceleration: 1.162,
      },
    ],
  },
];
