import debug from "debug";
import express from "express";
import fs from "fs/promises";
import http from "http";
import path from "path";
import { randomBytes } from "crypto";
import { Server as SocketIO } from "socket.io";

type UserToFollow = {
  socketId: string;
  username: string;
};
type OnUserFollowedPayload = {
  userToFollow: UserToFollow;
  action: "FOLLOW" | "UNFOLLOW";
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
const rawParser = express.raw({
  type: ["application/octet-stream", "application/octetstream", "*/*"],
  limit: "20mb",
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

app.use(express.json({ limit: "20mb" }));
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

app.get("/api/storage/scenes/:roomId", async (req, res) => {
  try {
    const roomId = req.params.roomId.replace(/[^a-zA-Z0-9_-]/g, "");
    if (!roomId) {
      res.status(400).json({ error: "invalid room id" });
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

app.put("/api/storage/scenes/:roomId", async (req, res) => {
  try {
    const roomId = req.params.roomId.replace(/[^a-zA-Z0-9_-]/g, "");
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

    const scenePath = getSafePathInDataRoot("scenes", `${roomId}.json`);
    await writeJson(scenePath, payload);
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "failed to save scene" });
  }
});

app.post("/api/storage/file", rawParser, async (req, res) => {
  try {
    const prefix = String(req.query.prefix || "");
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

app.get("/api/storage/file", async (req, res) => {
  try {
    const prefix = String(req.query.prefix || "");
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

app.post("/api/v2/post", rawParser, async (req, res) => {
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

app.get("/api/v2/:id", async (req, res) => {
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

app.post("/api/storage/migrations/scene/:id", rawParser, async (req, res) => {
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

  io.on("connection", (socket) => {
    ioDebug("connection established!");
    io.to(`${socket.id}`).emit("init-room");
    socket.on("join-room", async (roomID) => {
      socketDebug(`${socket.id} has joined ${roomID}`);
      await socket.join(roomID);
      const sockets = await io.in(roomID).fetchSockets();
      if (sockets.length <= 1) {
        io.to(`${socket.id}`).emit("first-in-room");
      } else {
        socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
        socket.broadcast.to(roomID).emit("new-user", socket.id);
      }

      io.in(roomID).emit(
        "room-user-change",
        sockets.map((socket) => socket.id),
      );
    });

    socket.on(
      "server-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends update to ${roomID}`);
        socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on(
      "server-volatile-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
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
          socket.broadcast.to(roomID).emit(
            "room-user-change",
            otherClients.map((socket) => socket.id),
          );
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
