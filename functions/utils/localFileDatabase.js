import { fileURLToPath } from 'node:url';

/**
 * 本地文件数据库适配器
 * 仅在 Node.js 运行时可用，用于容器/自托管场景下替代 KV/D1。
 */
class LocalFileDatabase {
    constructor(dbPath) {
        this.dbPath = this.normalizePath(dbPath || './data/local-db.json');
        this.dataLoaded = false;
        this.data = { files: {}, settings: {}, operations: {} };
        this.fsPromise = null;
        this.pathPromise = null;
    }

    async getFs() {
        if (!this.fsPromise) {
            if (typeof process === 'undefined' || !process.versions?.node) {
                throw new Error('LocalFileDatabase requires a Node.js runtime.');
            }
            this.fsPromise = import('node:fs/promises');
        }
        return this.fsPromise;
    }

    async getPath() {
        if (!this.pathPromise) {
            this.pathPromise = import('node:path');
        }
        return this.pathPromise;
    }

    normalizePath(inputPath) {
        // 允许使用 file:// 前缀
        if (inputPath && inputPath.startsWith('file:')) {
            return fileURLToPath(inputPath);
        }
        return inputPath;
    }

    async ensureLoaded() {
        if (this.dataLoaded) return;

        const fs = await this.getFs();
        const path = await this.getPath();
        const dir = path.dirname(this.dbPath);
        await fs.mkdir(dir, { recursive: true });

        try {
            const content = await fs.readFile(this.dbPath, 'utf-8');
            this.data = JSON.parse(content);
        } catch (err) {
            // 如果文件不存在则使用空白数据结构
            if (err.code !== 'ENOENT') {
                console.error('Failed to read local database file:', err);
            }
        }

        this.data.files = this.data.files || {};
        this.data.settings = this.data.settings || {};
        this.data.operations = this.data.operations || {};
        this.dataLoaded = true;
    }

    async persist() {
        const fs = await this.getFs();
        const path = await this.getPath();
        const dir = path.dirname(this.dbPath);
        await fs.mkdir(dir, { recursive: true });
        await fs.writeFile(this.dbPath, JSON.stringify(this.data, null, 2), 'utf-8');
    }

    /* ==================== 文件操作 ==================== */
    async putFile(fileId, value = '', options = {}) {
        await this.ensureLoaded();
        const metadata = options.metadata || {};
        this.data.files[fileId] = { value, metadata };
        await this.persist();
        return { success: true };
    }

    async getFile(fileId) {
        await this.ensureLoaded();
        const record = this.data.files[fileId];
        if (!record) return null;
        return { value: record.value, metadata: record.metadata || {} };
    }

    async getFileWithMetadata(fileId) {
        return await this.getFile(fileId);
    }

    async deleteFile(fileId) {
        await this.ensureLoaded();
        delete this.data.files[fileId];
        await this.persist();
        return { success: true };
    }

    async listFiles(options = {}) {
        await this.ensureLoaded();
        const prefix = options.prefix || '';
        const limit = options.limit || 1000;
        const cursor = options.cursor || null;

        const allKeys = Object.keys(this.data.files).filter((key) => key.startsWith(prefix)).sort();
        const startIndex = cursor ? allKeys.indexOf(cursor) + 1 : 0;
        const sliced = allKeys.slice(startIndex, startIndex + limit + 1);
        const hasMore = sliced.length > limit;
        const keys = (hasMore ? sliced.slice(0, limit) : sliced).map((name) => ({
            name,
            metadata: this.data.files[name]?.metadata || {}
        }));

        return {
            keys,
            cursor: hasMore ? keys[keys.length - 1].name : null,
            list_complete: !hasMore
        };
    }

    /* ==================== 设置操作 ==================== */
    async putSetting(key, value) {
        await this.ensureLoaded();
        this.data.settings[key] = value;
        await this.persist();
        return { success: true };
    }

    async getSetting(key) {
        await this.ensureLoaded();
        return this.data.settings[key] ?? null;
    }

    async deleteSetting(key) {
        await this.ensureLoaded();
        delete this.data.settings[key];
        await this.persist();
        return { success: true };
    }

    async listSettings(options = {}) {
        await this.ensureLoaded();
        const prefix = options.prefix || '';
        const limit = options.limit || 1000;
        const keys = Object.keys(this.data.settings)
            .filter((key) => key.startsWith(prefix))
            .sort()
            .slice(0, limit)
            .map((name) => ({ name, value: this.data.settings[name] }));

        return { keys };
    }

    /* ==================== 索引操作 ==================== */
    async putIndexOperation(operationId, operation) {
        await this.ensureLoaded();
        this.data.operations[operationId] = {
            type: operation.type,
            timestamp: operation.timestamp,
            data: operation.data,
            processed: operation.processed ?? false
        };
        await this.persist();
        return { success: true };
    }

    async getIndexOperation(operationId) {
        await this.ensureLoaded();
        const op = this.data.operations[operationId];
        if (!op) return null;
        return {
            type: op.type,
            timestamp: op.timestamp,
            data: op.data,
            processed: op.processed ?? false
        };
    }

    async deleteIndexOperation(operationId) {
        await this.ensureLoaded();
        delete this.data.operations[operationId];
        await this.persist();
        return { success: true };
    }

    async listIndexOperations(options = {}) {
        await this.ensureLoaded();
        const limit = options.limit || 1000;
        const processedFilter = options.processed;
        const entries = Object.entries(this.data.operations)
            .map(([id, op]) => ({ id, ...op }))
            .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0))
            .filter((op) => processedFilter === undefined || processedFilter === null ? true : op.processed === processedFilter)
            .slice(0, limit);

        return entries.map((op) => ({
            id: op.id,
            type: op.type,
            timestamp: op.timestamp,
            data: op.data,
            processed: op.processed ?? false
        }));
    }

    /* ==================== 通用方法 ==================== */
    async put(key, value, options) {
        if (key.startsWith('manage@sysConfig@')) {
            return this.putSetting(key, value);
        } else if (key.startsWith('manage@index@operation_')) {
            const operationId = key.replace('manage@index@operation_', '');
            const operation = JSON.parse(value);
            return this.putIndexOperation(operationId, operation);
        } else {
            return this.putFile(key, value, options);
        }
    }

    async get(key) {
        if (key.startsWith('manage@sysConfig@')) {
            return this.getSetting(key);
        } else if (key.startsWith('manage@index@operation_')) {
            const operationId = key.replace('manage@index@operation_', '');
            const operation = await this.getIndexOperation(operationId);
            return operation ? JSON.stringify(operation) : null;
        } else {
            const file = await this.getFile(key);
            return file ? file.value : null;
        }
    }

    async getWithMetadata(key) {
        if (key.startsWith('manage@sysConfig@')) {
            const value = await this.getSetting(key);
            return value ? { value, metadata: {} } : null;
        }
        return this.getFileWithMetadata(key);
    }

    async delete(key) {
        if (key.startsWith('manage@sysConfig@')) {
            return this.deleteSetting(key);
        } else if (key.startsWith('manage@index@operation_')) {
            const operationId = key.replace('manage@index@operation_', '');
            return this.deleteIndexOperation(operationId);
        } else {
            return this.deleteFile(key);
        }
    }

    async list(options = {}) {
        const prefix = options.prefix || '';
        if (prefix.startsWith('manage@sysConfig@')) {
            return this.listSettings(options);
        } else if (prefix.startsWith('manage@index@operation_')) {
            const operations = await this.listIndexOperations(options);
            const keys = operations.map((op) => ({ name: 'manage@index@operation_' + op.id }));
            return { keys };
        }
        return this.listFiles(options);
    }
}

export { LocalFileDatabase };
