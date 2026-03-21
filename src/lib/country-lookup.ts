// eslint-disable-next-line @typescript-eslint/no-explicit-any
let queryFn: ((point: [number, number]) => Record<string, any> | null) | null = null;
let loadPromise: Promise<void> | null = null;

async function ensureLoaded(): Promise<void> {
  if (queryFn) return;
  if (loadPromise) return loadPromise;

  loadPromise = (async () => {
    // which-polygon has no type declarations
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const whichPolygon = (await import("which-polygon" as string)).default;
    const response = await fetch("/data/countries-110m.json");
    const geojson = await response.json();
    queryFn = whichPolygon(geojson);
  })();

  return loadPromise;
}

export function getCountryForCoordinateSync(
  lat: number,
  lon: number
): string | null {
  if (!queryFn) return null;
  const result = queryFn([lon, lat]);
  return (result?.ADMIN as string) || null;
}

export async function preloadCountryData(): Promise<void> {
  await ensureLoaded();
}
