import debug from "debug";
import express from "express";
import fs from "fs/promises";
import http from "http";
import path from "path";
import { randomBytes } from "crypto";
import { Server as SocketIO } from "socket.io";
import type { Request, Response, NextFunction } from "express";

type UserToFollow = {
  socketId: string;
  username: string;
};
type OnUserFollowedPayload = {
  userToFollow: UserToFollow;
  action: "FOLLOW" | "UNFOLLOW";
};
type RoomUserRole = "owner" | "editor" | "viewer";
type RoomState = {
  ownerToken: string;
  ownerClaim: string;
  defaultJoinRole: "editor" | "viewer";
  tokenRoles: Map<string, RoomUserRole>;
  socketTokens: Map<string, string>;
};

const serverDebug = debug("server");
const ioDebug = debug("io");
const socketDebug = debug("socket");

require("dotenv").config(
  process.env.NODE_ENV !== "development"
    ? { path: ".env.production" }
    : { path: ".env.development" },
);

const app = express();
const port =
  process.env.PORT || (process.env.NODE_ENV !== "development" ? 80 : 3002); // default port to listen
const dataRoot = path.resolve(process.cwd(), "data");
const librariesRoot = path.resolve(process.cwd(), "libraries");
const roomCleanupDelayMs = Math.max(
  0,
  Number(process.env.ROOM_CLEANUP_DELAY_MS || 20 * 60 * 1000),
);
const transientDataTtlMs = Math.max(
  60000,
  Number(process.env.TRANSIENT_DATA_TTL_MS || 24 * 60 * 60 * 1000),
);
const transientCleanupSweepMs = Math.max(
  30000,
  Number(process.env.TRANSIENT_CLEANUP_SWEEP_MS || 15 * 60 * 1000),
);
const requestBodyLimit = process.env.REQUEST_BODY_LIMIT || "50mb";
const rawParser = express.raw({
  type: ["application/octet-stream", "application/octetstream", "*/*"],
  limit: requestBodyLimit,
});

const ensureDir = async (dir: string) => {
  await fs.mkdir(dir, { recursive: true });
};

const ensureDataLayout = async () => {
  await Promise.all([
    ensureDir(path.join(dataRoot, "scenes")),
    ensureDir(path.join(dataRoot, "files")),
    ensureDir(path.join(dataRoot, "share")),
    ensureDir(path.join(dataRoot, "migrations", "scenes")),
  ]);
};

const getSafePathInDataRoot = (...segments: string[]) => {
  const fullPath = path.resolve(dataRoot, ...segments);
  if (!fullPath.startsWith(dataRoot)) {
    throw new Error("Invalid path");
  }
  return fullPath;
};

const normalizePrefix = (prefix: string) => {
  const cleaned = prefix
    .replace(/\\/g, "/")
    .replace(/^\/+/, "")
    .replace(/\/+/g, "/");
  if (!cleaned || cleaned.includes("..")) {
    throw new Error("Invalid prefix");
  }
  return cleaned;
};

const getFilePathFromPrefixAndId = (prefix: string, id: string) => {
  const normalizedPrefix = normalizePrefix(prefix);
  const safeId = id.replace(/[^a-zA-Z0-9._-]/g, "");
  if (!safeId) {
    throw new Error("Invalid id");
  }
  const fullPath = getSafePathInDataRoot("files", normalizedPrefix, safeId);
  return fullPath;
};

const writeJson = async (targetPath: string, value: unknown) => {
  await ensureDir(path.dirname(targetPath));
  await fs.writeFile(targetPath, JSON.stringify(value), "utf8");
};

const readJson = async (targetPath: string) => {
  const text = await fs.readFile(targetPath, "utf8");
  return JSON.parse(text);
};

const extractUserToken = (req: Request) => {
  const authHeader = req.headers.authorization;
  if (typeof authHeader === "string") {
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (match?.[1]) {
      return match[1].trim();
    }
  }
  const headerToken = req.headers["x-user-token"];
  if (typeof headerToken === "string") {
    return headerToken.trim();
  }
  if (typeof req.query.userToken === "string") {
    return req.query.userToken.trim();
  }
  return "";
};

const isValidUserToken = (token: string) => /^[a-f0-9]{32,128}$/i.test(token);

const requireUserToken = (req: Request, res: Response, next: NextFunction) => {
  const token = extractUserToken(req);
  if (!isValidUserToken(token)) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }
  next();
};

const roomSockets = new Map<string, Set<string>>();
const socketRooms = new Map<string, Set<string>>();
const roomCleanupTimers = new Map<string, NodeJS.Timeout>();
const roomStates = new Map<string, RoomState>();
const stoppedRooms = new Set<string>();
let ioInstance: SocketIO | null = null;

const isRoomName = (roomID: string) => /^[a-zA-Z0-9_-]{1,100}$/.test(roomID);

const removeTrackedRoomForSocket = (socketId: string, roomID: string) => {
  const sockets = roomSockets.get(roomID);
  if (sockets) {
    sockets.delete(socketId);
    if (sockets.size === 0) {
      roomSockets.delete(roomID);
    }
  }
  const rooms = socketRooms.get(socketId);
  if (rooms) {
    rooms.delete(roomID);
    if (rooms.size === 0) {
      socketRooms.delete(socketId);
    }
  }
  const roomState = roomStates.get(roomID);
  if (roomState) {
    roomState.socketTokens.delete(socketId);
  }
};

const cleanupRoomData = async (roomID: string) => {
  try {
    await fs.rm(getSafePathInDataRoot("scenes", `${roomID}.json`), {
      force: true,
    });
    await fs.rm(getSafePathInDataRoot("files", "files", "rooms", roomID), {
      recursive: true,
      force: true,
    });
    roomStates.delete(roomID);
  } catch (error) {
    console.error(`failed to cleanup room data for ${roomID}`, error);
  }
};

const scheduleRoomCleanup = (roomID: string) => {
  const activeSockets = roomSockets.get(roomID);
  if (activeSockets && activeSockets.size > 0) {
    return;
  }
  if (roomCleanupTimers.has(roomID)) {
    return;
  }
  const timer = setTimeout(async () => {
    roomCleanupTimers.delete(roomID);
    const sockets = roomSockets.get(roomID);
    if (sockets && sockets.size > 0) {
      return;
    }
    await cleanupRoomData(roomID);
  }, roomCleanupDelayMs);
  roomCleanupTimers.set(roomID, timer);
};

const markSocketJoinedRoom = (socketId: string, roomID: string) => {
  const existingTimer = roomCleanupTimers.get(roomID);
  if (existingTimer) {
    clearTimeout(existingTimer);
    roomCleanupTimers.delete(roomID);
  }
  if (!roomSockets.has(roomID)) {
    roomSockets.set(roomID, new Set());
  }
  roomSockets.get(roomID)!.add(socketId);
  if (!socketRooms.has(socketId)) {
    socketRooms.set(socketId, new Set());
  }
  socketRooms.get(socketId)!.add(roomID);
};

const ensureRoomState = (
  roomID: string,
  ownerToken: string,
  ownerClaim: string,
  defaultJoinRole: "editor" | "viewer" = "editor",
): RoomState => {
  const existing = roomStates.get(roomID);
  if (existing) {
    return existing;
  }
  const state: RoomState = {
    ownerToken,
    ownerClaim,
    defaultJoinRole,
    tokenRoles: new Map([[ownerToken, "owner"]]),
    socketTokens: new Map(),
  };
  roomStates.set(roomID, state);
  return state;
};

const parseRoomIdFromPrefix = (prefix: string) => {
  const normalized = normalizePrefix(prefix);
  const match = normalized.match(/^files\/rooms\/([a-zA-Z0-9_-]+)(\/|$)/);
  return match?.[1] || null;
};

const canWriteRoomData = (roomID: string, token: string) => {
  const roomState = ensureRoomState(roomID, token, token);
  const role = roomState.tokenRoles.get(token);
  return role === "owner" || role === "editor";
};

const removeExpiredTransientData = async () => {
  const now = Date.now();
  const expireBefore = now - transientDataTtlMs;

  const shareRoot = getSafePathInDataRoot("share");
  const shareFilesRoot = getSafePathInDataRoot("files", "files", "shareLinks");
  const migrationScenesRoot = getSafePathInDataRoot("migrations", "scenes");

  try {
    const shareEntries = await fs.readdir(shareRoot, { withFileTypes: true });
    await Promise.all(
      shareEntries.map(async (entry) => {
        if (!entry.isFile() || !entry.name.endsWith(".bin")) {
          return;
        }
        const fullPath = path.join(shareRoot, entry.name);
        const stats = await fs.stat(fullPath);
        if (stats.mtimeMs > expireBefore) {
          return;
        }
        const id = entry.name.replace(/\.bin$/i, "");
        await fs.rm(fullPath, { force: true });
        await fs.rm(path.join(shareFilesRoot, id), {
          recursive: true,
          force: true,
        });
      }),
    );
  } catch (error: any) {
    if (error?.code !== "ENOENT") {
      console.error("failed to cleanup share data", error);
    }
  }

  try {
    const migrationEntries = await fs.readdir(migrationScenesRoot, {
      withFileTypes: true,
    });
    await Promise.all(
      migrationEntries.map(async (entry) => {
        if (!entry.isFile()) {
          return;
        }
        const fullPath = path.join(migrationScenesRoot, entry.name);
        const stats = await fs.stat(fullPath);
        if (stats.mtimeMs <= expireBefore) {
          await fs.rm(fullPath, { force: true });
        }
      }),
    );
  } catch (error: any) {
    if (error?.code !== "ENOENT") {
      console.error("failed to cleanup migration data", error);
    }
  }
};

const removeExpiredInactiveRoomData = async () => {
  const now = Date.now();
  const expireBefore = now - roomCleanupDelayMs;
  const scenesRoot = getSafePathInDataRoot("scenes");
  const roomFilesRoot = getSafePathInDataRoot("files", "files", "rooms");
  const activeRoomIds = new Set(
    Array.from(roomSockets.entries())
      .filter(([, sockets]) => sockets.size > 0)
      .map(([roomID]) => roomID),
  );

  try {
    const sceneEntries = await fs.readdir(scenesRoot, { withFileTypes: true });
    await Promise.all(
      sceneEntries.map(async (entry) => {
        if (!entry.isFile() || !entry.name.endsWith(".json")) {
          return;
        }
        const roomID = entry.name.replace(/\.json$/i, "");
        if (!isRoomName(roomID) || activeRoomIds.has(roomID)) {
          return;
        }
        const fullPath = path.join(scenesRoot, entry.name);
        const stats = await fs.stat(fullPath);
        if (stats.mtimeMs > expireBefore) {
          return;
        }
        await cleanupRoomData(roomID);
      }),
    );
  } catch (error: any) {
    if (error?.code !== "ENOENT") {
      console.error("failed to cleanup inactive room scenes", error);
    }
  }

  // Cleanup orphan room file directories that may remain without a scene file.
  try {
    const roomEntries = await fs.readdir(roomFilesRoot, { withFileTypes: true });
    await Promise.all(
      roomEntries.map(async (entry) => {
        if (!entry.isDirectory()) {
          return;
        }
        const roomID = entry.name;
        if (!isRoomName(roomID) || activeRoomIds.has(roomID)) {
          return;
        }
        const roomDir = path.join(roomFilesRoot, roomID);
        const stats = await fs.stat(roomDir);
        if (stats.mtimeMs > expireBefore) {
          return;
        }
        await fs.rm(roomDir, { recursive: true, force: true });
      }),
    );
  } catch (error: any) {
    if (error?.code !== "ENOENT") {
      console.error("failed to cleanup inactive room files", error);
    }
  }
};

type LibraryItem = {
  id: string;
  status?: "published" | "unpublished";
  [key: string]: any;
};

type LibrariesCache = {
  libraryItems: LibraryItem[];
  categories: Array<{ name: string; count: number }>;
  libraries: Array<{ id: string; name: string; category: string; count: number }>;
  loadedAt: number;
};

let librariesCache: LibrariesCache | null = null;

const normalizeCategory = (fileName: string) => {
  const base = fileName.replace(/\.excalidrawlib$/i, "");
  const token = base
    .replace(/[()]/g, "")
    .trim()
    .split(/[-_ ]+/)[0];
  if (!token) {
    return "general";
  }
  return token.toLowerCase();
};

const getLibraryBaseName = (fileName: string) =>
  fileName.replace(/\.excalidrawlib$/i, "");

const getLibraryDisplayName = (fileName: string) =>
  getLibraryBaseName(fileName)
    .replace(/\s*\(\d+\)\s*$/g, "")
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const loadLibrariesFromDisk = async (): Promise<LibrariesCache> => {
  const entries = await fs.readdir(librariesRoot, { withFileTypes: true });
  const files = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".excalidrawlib"))
    .map((entry) => entry.name);

  const allItems: LibraryItem[] = [];
  const categories = new Map<string, number>();
  const libraries: LibrariesCache["libraries"] = [];
  const seenIds = new Set<string>();

  await Promise.all(
    files.map(async (fileName) => {
      try {
        const fullPath = path.join(librariesRoot, fileName);
        const raw = await fs.readFile(fullPath, "utf8");
        const parsed = JSON.parse(raw) as {
          libraryItems?: LibraryItem[];
          library?: LibraryItem[];
        };
        const items = Array.isArray(parsed.libraryItems)
          ? parsed.libraryItems
          : Array.isArray(parsed.library)
            ? parsed.library
            : [];
        if (!items.length) {
          return;
        }

        const category = normalizeCategory(fileName);
        categories.set(category, (categories.get(category) || 0) + items.length);
        const libraryBaseName = getLibraryBaseName(fileName);
        const libraryDisplayName = getLibraryDisplayName(fileName);

        libraries.push({
          id: fileName,
          name: libraryDisplayName,
          category,
          count: items.length,
        });

        for (const item of items) {
          const rawId = item?.id?.trim?.();
          if (!rawId) {
            continue;
          }

          let itemId = rawId;
          if (seenIds.has(itemId)) {
            let counter = 1;
            itemId = `${libraryBaseName}__${rawId}`;
            while (seenIds.has(itemId)) {
              itemId = `${libraryBaseName}__${rawId}__${counter}`;
              counter += 1;
            }
          }
          seenIds.add(itemId);
          allItems.push({
            ...item,
            id: itemId,
            status: "published",
            category,
            sourceLibrary: libraryBaseName,
            sourceLibraryName: libraryDisplayName,
          });
        }
      } catch (error) {
        console.error(`failed to parse library file ${fileName}`, error);
      }
    }),
  );

  return {
    libraryItems: allItems,
    categories: Array.from(categories.entries()).map(([name, count]) => ({
      name,
      count,
    })),
    libraries: libraries.sort((a, b) => a.name.localeCompare(b.name)),
    loadedAt: Date.now(),
  };
};

const getLibrariesCache = async () => {
  if (!librariesCache) {
    librariesCache = await loadLibrariesFromDisk();
  }
  return librariesCache;
};

app.use(express.json({ limit: requestBodyLimit }));
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", process.env.CORS_ORIGIN || "*");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS");
  if (req.method === "OPTIONS") {
    res.sendStatus(200);
    return;
  }
  next();
});

app.use(express.static("public"));

app.get("/", (req, res) => {
  res.send("Excalidraw collaboration server is up :)");
});

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/libraries", async (_req, res) => {
  try {
    const data = await getLibrariesCache();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "failed to load libraries" });
  }
});

app.get("/api/storage/scenes/:roomId", requireUserToken, async (req, res) => {
  try {
    const roomId = req.params.roomId.replace(/[^a-zA-Z0-9_-]/g, "");
    if (!roomId) {
      res.status(400).json({ error: "invalid room id" });
      return;
    }
    if (stoppedRooms.has(roomId)) {
      res.status(410).json({ error: "room stopped" });
      return;
    }

    const scenePath = getSafePathInDataRoot("scenes", `${roomId}.json`);
    const scene = await readJson(scenePath);
    res.json(scene);
  } catch (error: any) {
    if (error?.code === "ENOENT") {
      res.status(404).json({ error: "scene not found" });
      return;
    }
    console.error(error);
    res.status(500).json({ error: "failed to load scene" });
  }
});

app.put("/api/storage/scenes/:roomId", requireUserToken, async (req, res) => {
  try {
    const roomId = req.params.roomId.replace(/[^a-zA-Z0-9_-]/g, "");
    const token = extractUserToken(req);
    if (stoppedRooms.has(roomId)) {
      res.status(410).json({ error: "room stopped" });
      return;
    }
    ensureRoomState(roomId, token, token);
    const payload = req.body as {
      sceneVersion?: number;
      iv?: string;
      ciphertext?: string;
    };
    if (
      !roomId ||
      typeof payload.sceneVersion !== "number" ||
      typeof payload.iv !== "string" ||
      typeof payload.ciphertext !== "string"
    ) {
      res.status(400).json({ error: "invalid payload" });
      return;
    }
    if (!canWriteRoomData(roomId, token)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    const scenePath = getSafePathInDataRoot("scenes", `${roomId}.json`);
    await writeJson(scenePath, payload);
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "failed to save scene" });
  }
});

app.post("/api/storage/file", requireUserToken, rawParser, async (req, res) => {
  try {
    const prefix = String(req.query.prefix || "");
    const token = extractUserToken(req);
    const roomId = parseRoomIdFromPrefix(prefix);
    if (roomId && stoppedRooms.has(roomId)) {
      res.status(410).json({ error: "room stopped" });
      return;
    }
    if (roomId && !canWriteRoomData(roomId, token)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }
    const id = String(req.query.id || "");
    const filePath = getFilePathFromPrefixAndId(prefix, id);

    await ensureDir(path.dirname(filePath));
    await fs.writeFile(filePath, Buffer.from(req.body));
    res.json({ ok: true });
  } catch (error: any) {
    if (error.message === "Invalid path" || error.message === "Invalid prefix") {
      res.status(400).json({ error: "invalid file path" });
      return;
    }
    console.error(error);
    res.status(500).json({ error: "failed to save file" });
  }
});

app.get("/api/storage/file", requireUserToken, async (req, res) => {
  try {
    const prefix = String(req.query.prefix || "");
    const roomId = parseRoomIdFromPrefix(prefix);
    if (roomId && stoppedRooms.has(roomId)) {
      res.status(410).json({ error: "room stopped" });
      return;
    }
    const id = String(req.query.id || "");
    const filePath = getFilePathFromPrefixAndId(prefix, id);

    const content = await fs.readFile(filePath);
    res.setHeader("Content-Type", "application/octet-stream");
    res.send(content);
  } catch (error: any) {
    if (error?.code === "ENOENT") {
      res.status(404).json({ error: "file not found" });
      return;
    }
    if (error.message === "Invalid path" || error.message === "Invalid prefix") {
      res.status(400).json({ error: "invalid file path" });
      return;
    }
    console.error(error);
    res.status(500).json({ error: "failed to read file" });
  }
});

app.post("/api/v2/post", requireUserToken, rawParser, async (req, res) => {
  try {
    const id = randomBytes(10).toString("hex");
    const sharePath = getSafePathInDataRoot("share", `${id}.bin`);
    await fs.writeFile(sharePath, Buffer.from(req.body));
    res.json({ id });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "failed to create share link" });
  }
});

app.get("/api/v2/:id", requireUserToken, async (req, res) => {
  try {
    const id = req.params.id.replace(/[^a-zA-Z0-9_-]/g, "");
    if (!id) {
      res.status(400).json({ error: "invalid id" });
      return;
    }
    const sharePath = getSafePathInDataRoot("share", `${id}.bin`);
    const content = await fs.readFile(sharePath);
    res.setHeader("Content-Type", "application/octet-stream");
    res.send(content);
  } catch (error: any) {
    if (error?.code === "ENOENT") {
      res.status(404).json({ error: "not found" });
      return;
    }
    console.error(error);
    res.status(500).json({ error: "failed to load share data" });
  }
});

app.post(
  "/api/storage/migrations/scene/:id",
  requireUserToken,
  rawParser,
  async (req, res) => {
  try {
    const id = req.params.id.replace(/[^a-zA-Z0-9_-]/g, "");
    if (!id) {
      res.status(400).json({ error: "invalid id" });
      return;
    }
    const metadata = typeof req.query.metadata === "string" ? req.query.metadata : "{}";
    const scenePath = getSafePathInDataRoot("migrations", "scenes", `${id}.bin`);
    const metaPath = getSafePathInDataRoot("migrations", "scenes", `${id}.json`);
    await fs.writeFile(scenePath, Buffer.from(req.body));
    await writeJson(metaPath, { metadata, created: Date.now() });
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "failed to save migration scene" });
  }
  },
);

app.post("/api/session/stop", async (req, res) => {
  try {
    const roomId = String(req.body?.roomId || "").replace(/[^a-zA-Z0-9_-]/g, "");
    const requestTokenFromBody =
      typeof req.body?.userToken === "string" ? req.body.userToken.trim() : "";
    const requestToken = extractUserToken(req) || requestTokenFromBody;
    if (!isRoomName(roomId) || !isValidUserToken(requestToken)) {
      res.status(400).json({ error: "invalid request" });
      return;
    }

    const roomState = roomStates.get(roomId);
    if (!roomState) {
      res.json({ ok: true });
      return;
    }
    if (roomState.ownerToken !== requestToken) {
      res.status(403).json({ error: "forbidden" });
      return;
    }

    stoppedRooms.add(roomId);
    await cleanupRoomData(roomId);
    roomStates.delete(roomId);
    ioInstance?.in(roomId).emit("session-stopped", { roomId });
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "failed to stop session" });
  }
});

const server = http.createServer(app);

server.listen(port, () => {
  serverDebug(`listening on port: ${port}`);
});

try {
  const io = new SocketIO(server, {
    transports: ["websocket", "polling"],
    cors: {
      allowedHeaders: ["Content-Type", "Authorization"],
      origin: process.env.CORS_ORIGIN || "*",
      credentials: true,
    },
    allowEIO3: true,
  });
  ioInstance = io;

  io.on("connection", (socket) => {
    const token =
      typeof socket.handshake.auth?.userToken === "string"
        ? socket.handshake.auth.userToken.trim()
        : "";
    if (!isValidUserToken(token)) {
      socket.emit("unauthorized");
      socket.disconnect();
      return;
    }
    socket.data.userToken = token;

    const emitRoomState = async (roomID: string) => {
      const roomState = roomStates.get(roomID);
      if (!roomState) {
        return;
      }
      const sockets = await io.in(roomID).fetchSockets();
      // One participant per token so opening same room in another tab doesn't
      // create duplicate attendee rows.
      const participantsByToken = new Map<
        string,
        { socketId: string; role: RoomUserRole; socketIds: string[] }
      >();
      for (const roomSocket of sockets) {
        const participantToken = roomState.socketTokens.get(roomSocket.id);
        if (!participantToken) {
          continue;
        }
        const existing = participantsByToken.get(participantToken);
        if (existing) {
          existing.socketIds.push(roomSocket.id);
          continue;
        }
        participantsByToken.set(participantToken, {
          socketId: roomSocket.id,
          role: roomState.tokenRoles.get(participantToken) || "editor",
          socketIds: [roomSocket.id],
        });
      }
      const participants = Array.from(participantsByToken.values());

      // Emit per-socket so each tab receives its own resolved role even when
      // the representative participant socketId belongs to another tab.
      for (const roomSocket of sockets) {
        const participantToken = roomState.socketTokens.get(roomSocket.id);
        roomSocket.emit("room-state", {
          roomId: roomID,
          defaultJoinRole: roomState.defaultJoinRole,
          participants,
          selfRole: participantToken
            ? roomState.tokenRoles.get(participantToken) || "editor"
            : "editor",
        });
      }
    };

    const emitRoomUserChange = async (roomID: string) => {
      const sockets = await io.in(roomID).fetchSockets();
      const roomState = roomStates.get(roomID);
      if (!roomState) {
        io.in(roomID).emit("room-user-change", sockets.map((s) => s.id));
        return;
      }
      const seenTokens = new Set<string>();
      const dedupedSocketIds: string[] = [];
      for (const roomSocket of sockets) {
        const tokenForSocket = roomState.socketTokens.get(roomSocket.id);
        if (!tokenForSocket) {
          continue;
        }
        if (seenTokens.has(tokenForSocket)) {
          continue;
        }
        seenTokens.add(tokenForSocket);
        dedupedSocketIds.push(roomSocket.id);
      }
      io.in(roomID).emit("room-user-change", dedupedSocketIds);
    };

    ioDebug("connection established!");
    io.to(`${socket.id}`).emit("init-room");
    socket.on(
      "join-room",
      async (payload: string | { roomId: string; defaultJoinRole?: string; ownerClaim?: string }) => {
        const roomID =
          typeof payload === "string"
            ? payload
            : String(payload?.roomId || "");
        if (!isRoomName(roomID)) {
          socket.emit("invalid-room");
          return;
        }
        if (stoppedRooms.has(roomID)) {
          socket.emit("session-stopped", { roomId: roomID });
          return;
        }
        const requestedDefaultJoinRole =
          typeof payload === "object" && payload?.defaultJoinRole === "viewer"
            ? "viewer"
            : "editor";
        const ownerClaim =
          typeof payload === "object" && typeof payload?.ownerClaim === "string"
            ? payload.ownerClaim
            : "";
        const existingRoomState = roomStates.get(roomID);
        const roomState = ensureRoomState(
          roomID,
          socket.data.userToken,
          ownerClaim || socket.data.userToken,
          requestedDefaultJoinRole,
        );
        if (
          !existingRoomState ||
          roomState.ownerToken === socket.data.userToken
        ) {
          roomState.defaultJoinRole = requestedDefaultJoinRole;
        }
        if (ownerClaim && roomState.ownerClaim === ownerClaim) {
          Array.from(roomState.tokenRoles.entries()).forEach(([token, role]) => {
            if (role === "owner") {
              roomState.tokenRoles.set(token, "editor");
            }
          });
          roomState.ownerToken = socket.data.userToken;
          roomState.tokenRoles.set(socket.data.userToken, "owner");
        }
        if (!roomState.tokenRoles.has(socket.data.userToken)) {
          roomState.tokenRoles.set(socket.data.userToken, roomState.defaultJoinRole);
        }
        roomState.socketTokens.set(socket.id, socket.data.userToken);
        socketDebug(`${socket.id} has joined ${roomID}`);
        await socket.join(roomID);
        markSocketJoinedRoom(socket.id, roomID);
        const sockets = await io.in(roomID).fetchSockets();
        if (sockets.length <= 1) {
          io.to(`${socket.id}`).emit("first-in-room");
        } else {
          socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
          socket.broadcast.to(roomID).emit("new-user", socket.id);
        }

        await emitRoomUserChange(roomID);
        await emitRoomState(roomID);
      },
    );

    socket.on(
      "server-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        if (!isRoomName(roomID) || !socket.rooms.has(roomID)) {
          return;
        }
        const roomState = roomStates.get(roomID);
        const tokenInRoom = roomState?.socketTokens.get(socket.id);
        const role = tokenInRoom ? roomState?.tokenRoles.get(tokenInRoom) : null;
        if (role === "viewer") {
          socket.emit("room-permission-error", {
            roomId: roomID,
            message: "viewer-cannot-edit",
          });
          return;
        }
        socketDebug(`${socket.id} sends update to ${roomID}`);
        socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on(
      "set-user-role",
      async (payload: {
        roomId: string;
        targetSocketId: string;
        role: RoomUserRole;
      }) => {
        const roomID = payload.roomId;
        if (!isRoomName(roomID)) {
          return;
        }
        const roomState = roomStates.get(roomID);
        if (!roomState) {
          return;
        }
        if (roomState.ownerToken !== socket.data.userToken) {
          socket.emit("room-permission-error", {
            roomId: roomID,
            message: "only-owner-can-manage-roles",
          });
          return;
        }
        if (
          payload.role !== "owner" &&
          payload.role !== "editor" &&
          payload.role !== "viewer"
        ) {
          return;
        }
        const targetToken = roomState.socketTokens.get(payload.targetSocketId);
        if (!targetToken || targetToken === roomState.ownerToken) {
          return;
        }
        roomState.tokenRoles.set(targetToken, payload.role);
        await emitRoomState(roomID);
      },
    );

    socket.on(
      "set-all-user-roles",
      async (payload: { roomId: string; role: "editor" | "viewer" }) => {
        const roomID = payload.roomId;
        if (!isRoomName(roomID)) {
          return;
        }
        const roomState = roomStates.get(roomID);
        if (!roomState || roomState.ownerToken !== socket.data.userToken) {
          return;
        }
        Array.from(roomState.tokenRoles.entries()).forEach(([token]) => {
          if (token === roomState.ownerToken) {
            return;
          }
          roomState.tokenRoles.set(token, payload.role);
        });
        await emitRoomState(roomID);
      },
    );

    socket.on("stop-session", async (payload: { roomId: string }) => {
      const roomID = payload.roomId;
      if (!isRoomName(roomID)) {
        return;
      }
      const roomState = roomStates.get(roomID);
      if (!roomState || roomState.ownerToken !== socket.data.userToken) {
        return;
      }
      stoppedRooms.add(roomID);
      await cleanupRoomData(roomID);
      roomStates.delete(roomID);
      io.in(roomID).emit("session-stopped", { roomId: roomID });
    });

    socket.on(
      "server-volatile-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        if (!isRoomName(roomID) || !socket.rooms.has(roomID)) {
          return;
        }
        socketDebug(`${socket.id} sends volatile update to ${roomID}`);
        socket.volatile.broadcast
          .to(roomID)
          .emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on("user-follow", async (payload: OnUserFollowedPayload) => {
      const roomID = `follow@${payload.userToFollow.socketId}`;

      switch (payload.action) {
        case "FOLLOW": {
          await socket.join(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
        case "UNFOLLOW": {
          await socket.leave(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
      }
    });

    socket.on("disconnecting", async () => {
      socketDebug(`${socket.id} has disconnected`);
      for (const roomID of Array.from(socket.rooms)) {
        const otherClients = (await io.in(roomID).fetchSockets()).filter(
          (_socket) => _socket.id !== socket.id,
        );

        const isFollowRoom = roomID.startsWith("follow@");

        if (!isFollowRoom && otherClients.length > 0) {
          await emitRoomUserChange(roomID);
        }

        if (!isFollowRoom && roomID !== socket.id) {
          removeTrackedRoomForSocket(socket.id, roomID);
          await emitRoomState(roomID);
          scheduleRoomCleanup(roomID);
        }

        if (isFollowRoom && otherClients.length === 0) {
          const socketId = roomID.replace("follow@", "");
          io.to(socketId).emit("broadcast-unfollow");
        }
      }
    });

    socket.on("disconnect", () => {
      socket.removeAllListeners();
      socket.disconnect();
    });
  });
} catch (error) {
  console.error(error);
}

ensureDataLayout().catch((error) => {
  console.error("failed to initialize data directories", error);
});

Promise.all([removeExpiredTransientData(), removeExpiredInactiveRoomData()]).catch(
  (error) => {
    console.error("failed to run startup cleanup", error);
  },
);

setInterval(() => {
  Promise.all([removeExpiredTransientData(), removeExpiredInactiveRoomData()]).catch(
    (error) => {
      console.error("failed to cleanup transient/inactive room data", error);
    },
  );
}, transientCleanupSweepMs);
