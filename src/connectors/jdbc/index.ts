import { spawn, type ChildProcess } from "child_process";
import { createInterface } from "readline";
import fs from "fs";
import path from "path";
import {
  Connector,
  ConnectorType,
  ConnectorRegistry,
  DSNParser,
  SQLResult,
  TableColumn,
  TableIndex,
  StoredProcedure,
  ExecuteOptions,
  ConnectorConfig,
} from "../interface.js";
import { obfuscateDSNPassword } from "../../utils/dsn-obfuscate.js";

/**
 * Find a Java executable on the system.
 * Checks JAVA_HOME first, then falls back to "java" on PATH.
 */
function findJava(): string {
  const javaHome = process.env.JAVA_HOME;
  if (javaHome) {
    const javaBin = process.platform === "win32"
      ? path.join(javaHome, "bin", "java.exe")
      : path.join(javaHome, "bin", "java");
    if (fs.existsSync(javaBin)) return javaBin;
  }
  return "java";
}

/**
 * Get the path to the compiled JdbcBridge class.
 * Looks relative to the connector source directory.
 */
function getBridgeClassPath(): string {
  // __dirname points to dist/connectors/jdbc/ after build
  // The Java bridge source is at src/connectors/jdbc/JdbcBridge.java
  // For production use, the bridge should be pre-compiled and placed alongside
  const possiblePaths = [
    // Priority 1: Explicit DBHUB_BRIDGE_DIR env var (for Docker)
    process.env.DBHUB_BRIDGE_DIR,
    // Priority 2: Docker standard location
    "/app/bridge",
    // Priority 3: Development: bridge compiled in source tree
    path.join(process.cwd(), "src", "connectors", "jdbc"),
    // Priority 4: Relative to dist (compiled bridge class alongside JS)
    path.join(path.dirname(new URL(import.meta.url).pathname)),
  ].filter(Boolean) as string[];

  for (const p of possiblePaths) {
    if (fs.existsSync(path.join(p, "JdbcBridge.class"))) {
      return p;
    }
  }

  // Default: assume bridge is in the same directory as the JS file
  return path.join(path.dirname(new URL(import.meta.url).pathname));
}

/**
 * Resolve the driver JAR path from source config.
 * Supports:
 * - Absolute paths: /app/drivers/updb6-jdbc.jar
 * - Relative paths: resolved against cwd
 * - Environment variables: ${DRIVER_PATH}
 */
function resolveDriverPath(driverPath?: string): string | null {
  if (!driverPath) return null;

  // Expand environment variables
  let resolved = driverPath.replace(/\$\{([^}]+)\}/g, (_, name) => process.env[name] || "");

  if (path.isAbsolute(resolved)) {
    return resolved;
  }

  // Resolve relative to cwd
  const absolute = path.resolve(process.cwd(), resolved);
  if (fs.existsSync(absolute)) return absolute;

  // Also try relative to script location
  const scriptDir = path.dirname(new URL(import.meta.url).pathname);
  const scriptRelative = path.resolve(scriptDir, "..", "..", "..", resolved);
  if (fs.existsSync(scriptRelative)) return scriptRelative;

  return absolute; // return the path so error message can report it
}

/**
 * Parse JDBC URL to extract host, port, and database.
 * Handles formats like: jdbc:updb://host:port/database
 */
function parseJdbcUrl(jdbcUrl: string): { type: string; host?: string; port?: number; database?: string } {
  // Strip jdbc: prefix
  const withoutPrefix = jdbcUrl.replace(/^jdbc:/, "");
  const [dbType] = withoutPrefix.split(":", 1);

  try {
    // Try to parse as URL: updb://host:port/database
    const urlMatch = withoutPrefix.match(/^(\w+):\/\/([^:/]+)(?::(\d+))?(?:\/(.*))?$/);
    if (urlMatch) {
      return {
        type: urlMatch[1],
        host: urlMatch[2],
        port: urlMatch[3] ? parseInt(urlMatch[3], 10) : undefined,
        database: urlMatch[4] || undefined,
      };
    }
  } catch {
    // fall through
  }

  return { type: dbType };
}

class JdbcDSNParser implements DSNParser {
  isValidDSN(dsn: string): boolean {
    return dsn.startsWith("jdbc:");
  }

  getSampleDSN(): string {
    return "jdbc:updb://192.168.1.100:5999/UPDB";
  }

  async parse(dsn: string): Promise<object> {
    return { jdbcUrl: dsn };
  }
}

class JdbcConnector implements Connector {
  id: ConnectorType = "jdbc";
  name = "JDBC Generic";
  dsnParser = new JdbcDSNParser();

  private sourceId = "";
  private bridgeProcess: ChildProcess | null = null;
  private responseResolvers: Array<{ resolve: (value: any) => void; reject: (err: Error) => void }> = [];
  private lineReader: ReturnType<typeof createInterface> | null = null;
  private stderrLines: string[] = [];
  private driverClass = "";
  private jdbcUrl = "";
  private username = "";
  private password = "";
  private maxConnections = 5;
  private timeoutMs = 30000;

  getId(): string {
    return this.sourceId;
  }

  clone(): Connector {
    return new JdbcConnector();
  }

  async connect(dsn: string, _initScript?: string, config?: ConnectorConfig): Promise<void> {
    this.jdbcUrl = dsn;

    // Use the source config that was attached by ConnectorManager
    const sourceConfig = (this as any)._sourceConfig;
    if (sourceConfig) {
      this.driverClass = sourceConfig.driver_class || "";
      this.username = sourceConfig.user || "";
      this.password = sourceConfig.password || "";
      this.maxConnections = sourceConfig.max_connections || 5;
      this.timeoutMs = sourceConfig.timeout || 30000;
    }

    if (!this.driverClass) {
      throw new Error("JDBC driver_class is required for JDBC connector");
    }

    // Find the driver JAR
    const driverPath = sourceConfig?.driver_path
      ? resolveDriverPath(sourceConfig.driver_path)
      : null;

    if (driverPath && !fs.existsSync(driverPath)) {
      throw new Error(
        `JDBC driver JAR not found at: ${driverPath}. ` +
        `Set driver_path in the source config to the correct JAR file location.`
      );
    }

    // Find Java
    const javaBin = findJava();

    // Build classpath: driver JAR + bridge class directory
    const bridgeDir = getBridgeClassPath();
    const classpathParts = [bridgeDir];
    if (driverPath) {
      classpathParts.push(driverPath);
    }
    const classpath = classpathParts.join(path.delimiter);

    // Build Java command
    const javaArgs = [
      "-cp", classpath,
      "JdbcBridge",
      this.driverClass,
      this.jdbcUrl,
      this.username,
      this.password,
      String(this.maxConnections),
      String(this.timeoutMs),
    ];

    console.error(`[JDBC] Starting Java bridge: ${javaBin} ${javaArgs.map(a => a.includes(" ") ? `"${a}"` : a).join(" ")}`);
    console.error(`[JDBC] Driver class: ${this.driverClass}`);
    console.error(`[JDBC] JDBC URL: ${obfuscateDSNPassword(this.jdbcUrl)}`);

    return new Promise<void>((resolve, reject) => {
      try {
        this.bridgeProcess = spawn(javaBin, javaArgs, {
          stdio: ["pipe", "pipe", "pipe"],
        });
      } catch (err) {
        reject(new Error(
          `Failed to start Java bridge. Is Java installed? ` +
          `Set JAVA_HOME or ensure "java" is on PATH. Error: ${(err as Error).message}`
        ));
        return;
      }

      const proc = this.bridgeProcess!;
      let startupResolved = false;

      // Read stdout line by line
      this.lineReader = createInterface({ input: proc.stdout! });
      this.lineReader.on("line", (line: string) => {
        try {
          const msg = JSON.parse(line);

          // First response is the startup ok/error
          if (!startupResolved) {
            startupResolved = true;
            if (msg.type === "ok") {
              resolve();
            } else if (msg.type === "error") {
              reject(new Error(`JDBC bridge startup error: ${msg.message}`));
            } else {
              reject(new Error(`Unexpected JDBC bridge startup response: ${line}`));
            }
            return;
          }

          // Subsequent responses use FIFO queue (bridge processes commands sequentially)
          const resolver = this.responseResolvers.shift();
          if (resolver) {
            if (msg.type === "error") {
              resolver.reject(new Error(msg.message));
            } else {
              resolver.resolve(msg);
            }
          }
        } catch {
          // Not JSON or parse error — may be unexpected output
          console.error(`[JDBC Bridge stdout]: ${line}`);
        }
      });

      // Collect stderr for diagnostics
      proc.stderr!.on("data", (data: Buffer) => {
        const text = data.toString().trim();
        if (text) {
          this.stderrLines.push(text);
          console.error(`[JDBC Bridge stderr]: ${text}`);
        }
      });

      proc.on("error", (err: Error) => {
        if (!startupResolved) {
          startupResolved = true;
          reject(new Error(`Failed to start Java bridge: ${err.message}`));
        }
      });

      proc.on("exit", (code: number | null) => {
        if (!startupResolved) {
          startupResolved = true;
          const stderr = this.stderrLines.join("\n");
          reject(new Error(
            `Java bridge exited with code ${code} during startup. ` +
            `Stderr: ${stderr || "(none)"}`
          ));
        }
        // Reject any pending requests
        while (this.responseResolvers.length > 0) {
          const resolver = this.responseResolvers.shift()!;
          resolver.reject(new Error(`Java bridge exited unexpectedly (code ${code})`));
        }
      });
    });
  }

  async disconnect(): Promise<void> {
    if (this.bridgeProcess && !this.bridgeProcess.killed) {
      try {
        // Send disconnect command
        this.bridgeProcess.stdin?.write(JSON.stringify({ action: "disconnect" }) + "\n");
        // Give it a moment to process, then kill
        await new Promise<void>((resolve) => {
          setTimeout(() => {
            try {
              this.bridgeProcess?.kill();
            } catch {}
            resolve();
          }, 1000);
        });
      } catch {
        try { this.bridgeProcess.kill(); } catch {}
      }
    }
    this.bridgeProcess = null;
    this.lineReader?.close();
    this.lineReader = null;
    this.responseResolvers.length = 0;
    this.stderrLines = [];
  }

  private async sendCommand(action: string, params: Record<string, any> = {}): Promise<any> {
    if (!this.bridgeProcess || this.bridgeProcess.killed) {
      throw new Error("JDBC bridge is not connected");
    }

    const command = JSON.stringify({ action, ...params });

    return new Promise<any>((resolve, reject) => {
      this.responseResolvers.push({ resolve, reject });

      // Set timeout
      const timer = setTimeout(() => {
        const idx = this.responseResolvers.findIndex(r => r.resolve === resolve);
        if (idx !== -1) this.responseResolvers.splice(idx, 1);
        reject(new Error(`JDBC bridge command '${action}' timed out after ${this.timeoutMs * 2}ms`));
      }, this.timeoutMs * 2); // 2x timeout for command

      try {
        this.bridgeProcess!.stdin!.write(command + "\n");
      } catch (err) {
        clearTimeout(timer);
        const idx = this.responseResolvers.findIndex(r => r.resolve === resolve);
        if (idx !== -1) this.responseResolvers.splice(idx, 1);
        reject(new Error(`Failed to send command to JDBC bridge: ${(err as Error).message}`));
      }
    });
  }

  async getSchemas(): Promise<string[]> {
    const resp = await this.sendCommand("schemas");
    return resp.schemas || [];
  }

  async getDefaultSchema(): Promise<string | null> {
    const schemas = await this.getSchemas();
    // Return the first non-system schema, or the first schema
    const userSchemas = schemas.filter(s => !s.startsWith("pg_") && s !== "information_schema" && s !== "sys");
    return userSchemas[0] || schemas[0] || null;
  }

  async getTables(schema?: string): Promise<string[]> {
    const resp = await this.sendCommand("tables", { schema: schema || null });
    return resp.tables || [];
  }

  async getViews(schema?: string): Promise<string[]> {
    const resp = await this.sendCommand("views", { schema: schema || null });
    return resp.views || [];
  }

  async getTableSchema(tableName: string, schema?: string): Promise<TableColumn[]> {
    const resp = await this.sendCommand("columns", { schema: schema || null, table: tableName });
    return (resp.columns || []).map((col: any) => ({
      column_name: col.column_name || "",
      data_type: col.data_type || "",
      is_nullable: col.is_nullable || "NO",
      column_default: col.column_default || null,
      description: col.description || null,
    }));
  }

  async tableExists(tableName: string, schema?: string): Promise<boolean> {
    const resp = await this.sendCommand("table_exists", { schema: schema || null, table: tableName });
    return resp.exists === true;
  }

  async getTableIndexes(tableName: string, schema?: string): Promise<TableIndex[]> {
    const resp = await this.sendCommand("indexes", { schema: schema || null, table: tableName });
    return (resp.indexes || []).map((idx: any) => ({
      index_name: idx.index_name || "",
      column_names: idx.column_names || [],
      is_unique: idx.is_unique === true,
      is_primary: idx.is_primary === true,
    }));
  }

  async getStoredProcedures(schema?: string, routineType?: "procedure" | "function"): Promise<string[]> {
    const resp = await this.sendCommand("procedures", { schema: schema || null });
    return resp.procedures || [];
  }

  async getStoredProcedureDetail(procedureName: string, schema?: string): Promise<StoredProcedure> {
    const resp = await this.sendCommand("procedure_detail", { schema: schema || null, name: procedureName });
    const detail = resp.detail || {};
    return {
      procedure_name: detail.procedure_name || procedureName,
      procedure_type: detail.procedure_type || "procedure",
      language: detail.language || "SQL",
      parameter_list: detail.parameter_list || "",
      return_type: detail.return_type,
      definition: detail.definition,
    };
  }

  async getTableRowCount(tableName: string, schema?: string): Promise<number | null> {
    const resp = await this.sendCommand("table_row_count", { schema: schema || null, table: tableName });
    return resp.count !== undefined ? resp.count : null;
  }

  async getTableComment(tableName: string, schema?: string): Promise<string | null> {
    const resp = await this.sendCommand("table_comment", { schema: schema || null, table: tableName });
    return resp.comment || null;
  }

  async executeSQL(sql: string, options: ExecuteOptions, parameters?: any[]): Promise<SQLResult> {
    const resp = await this.sendCommand("query", {
      sql,
      params: parameters || null,
      readonly: options.readonly || false,
      maxRows: options.maxRows || null,
    });

    return {
      rows: resp.rows || [],
      rowCount: resp.rowCount ?? (resp.rows?.length || 0),
    };
  }

  /**
   * Attach source config for use during connect().
   * Called by ConnectorManager before connect().
   */
  setSourceConfig(config: Record<string, any>): void {
    (this as any)._sourceConfig = config;
  }
}

const jdbcConnector = new JdbcConnector();
ConnectorRegistry.register(jdbcConnector);
