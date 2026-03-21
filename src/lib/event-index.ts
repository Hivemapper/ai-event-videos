export interface EventIndexEntry {
  eventId: string;
  lat: number;
  lon: number;
  country: string | null;
  roadClass: string | null;
  roadLabel: string | null;
}

const DB_NAME = "event-index";
const DB_VERSION = 1;
const STORE_NAME = "events";

function openDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "eventId" });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export async function getIndexedEvents(
  eventIds: string[]
): Promise<Map<string, EventIndexEntry>> {
  const db = await openDB();
  const tx = db.transaction(STORE_NAME, "readonly");
  const store = tx.objectStore(STORE_NAME);
  const results = new Map<string, EventIndexEntry>();

  await Promise.all(
    eventIds.map(
      (id) =>
        new Promise<void>((resolve) => {
          const request = store.get(id);
          request.onsuccess = () => {
            if (request.result) {
              results.set(id, request.result);
            }
            resolve();
          };
          request.onerror = () => resolve();
        })
    )
  );

  db.close();
  return results;
}

export async function putIndexedEvents(
  entries: EventIndexEntry[]
): Promise<void> {
  if (entries.length === 0) return;
  const db = await openDB();
  const tx = db.transaction(STORE_NAME, "readwrite");
  const store = tx.objectStore(STORE_NAME);

  for (const entry of entries) {
    store.put(entry);
  }

  await new Promise<void>((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });

  db.close();
}

export async function getAllIndexedEvents(): Promise<
  Map<string, EventIndexEntry>
> {
  const db = await openDB();
  const tx = db.transaction(STORE_NAME, "readonly");
  const store = tx.objectStore(STORE_NAME);

  return new Promise((resolve, reject) => {
    const request = store.getAll();
    request.onsuccess = () => {
      const results = new Map<string, EventIndexEntry>();
      for (const entry of request.result) {
        results.set(entry.eventId, entry);
      }
      db.close();
      resolve(results);
    };
    request.onerror = () => {
      db.close();
      reject(request.error);
    };
  });
}
