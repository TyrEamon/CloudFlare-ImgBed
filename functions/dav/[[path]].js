// WebDAV 服务支持
import { fetchSecurityConfig, fetchOthersConfig } from "../utils/sysConfig";

const webDavMethods = 'OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, MKCOL';
const webDavAllowedHeaders = [
    'Authorization',
    'Content-Type',
    'Depth',
    'Destination',
    'Overwrite',
    'Range',
    'If-Match',
    'If-None-Match',
    'X-Requested-With',
].join(', ');
const webDavExposedHeaders = [
    'Accept-Ranges',
    'Allow',
    'Content-Length',
    'Content-Range',
    'DAV',
    'ETag',
    'Location',
    'MS-Author-Via',
    'WWW-Authenticate',
].join(', ');

export async function onRequest(context) {
    const { request, env } = context;
    const othersConfig = await fetchOthersConfig(env);

    if (request.method === 'OPTIONS') return handleOptions(request, othersConfig);

    const authResponse = checkAuth(request, othersConfig);
    if (authResponse) return withCorsHeaders(authResponse, request, othersConfig);

    // 从请求路径中替换第一个 /dav 部分
    const url = new URL(request.url);
    url.pathname = url.pathname.replace(/^\/dav/, '') || '/';
    const modifiedRequest = new Request(url.toString(), request);
    const rootDir = normalizeRootDir(othersConfig.webDAV?.rootDir || '');

    let response;
    switch (modifiedRequest.method) {
        case 'PROPFIND':
            response = await handlePropfind(modifiedRequest, env, rootDir);
            break;
        case 'PUT':
            response = await handlePut(modifiedRequest, env, rootDir, othersConfig);
            break;
        case 'DELETE':
            response = await handleDelete(modifiedRequest, env, rootDir);
            break;
        case 'GET':
        case 'HEAD':
            response = await handleGet(modifiedRequest, env, rootDir);
            break;
        case 'MKCOL':
            response = new Response(null, { status: 201 });
            break;
        default:
            response = new Response('Method Not Allowed', { status: 405 });
    }

    return withCorsHeaders(response, request, othersConfig);
}

// --- UTILITY FUNCTIONS ---

function getCorsHeaders(request, othersConfig) {
    const origin = request.headers.get('Origin');
    const allowedOrigin = getAllowedCorsOrigin(origin, othersConfig);
    const headers = {
        'Access-Control-Allow-Origin': allowedOrigin,
        'Access-Control-Allow-Methods': webDavMethods,
        'Access-Control-Allow-Headers': webDavAllowedHeaders,
        'Access-Control-Expose-Headers': webDavExposedHeaders,
        'Access-Control-Max-Age': '86400',
    };

    if (origin && allowedOrigin !== 'null') {
        headers['Access-Control-Allow-Credentials'] = 'true';
    }

    return headers;
}

function getAllowedCorsOrigin(origin, othersConfig) {
    if (!origin) return '*';

    const configuredOrigins = othersConfig.webDAV?.corsOrigins || '';
    const allowedOrigins = configuredOrigins
        .split(',')
        .map(normalizeOrigin)
        .filter(Boolean);

    if (allowedOrigins.length === 0 || allowedOrigins.includes('*') || allowedOrigins.includes(origin)) {
        return origin;
    }

    return 'null';
}

function normalizeOrigin(origin) {
    const value = (origin || '').trim().replace(/\/+$/, '');
    if (value === '*') return value;

    try {
        return new URL(value).origin;
    } catch {
        return '';
    }
}

function appendVary(headers, value) {
    const existing = headers.get('Vary');
    const values = existing ? existing.split(',').map(item => item.trim()) : [];
    if (!values.some(item => item.toLowerCase() === value.toLowerCase())) {
        values.push(value);
    }
    headers.set('Vary', values.join(', '));
}

function withCorsHeaders(response, request, othersConfig) {
    const headers = new Headers(response.headers);
    const corsHeaders = getCorsHeaders(request, othersConfig);
    Object.entries(corsHeaders).forEach(([key, value]) => headers.set(key, value));
    appendVary(headers, 'Origin');

    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers,
    });
}

function normalizeRootDir(dir) {
    return normalizeRelativePath(dir);
}

function normalizeRelativePath(path) {
    return (path || '')
        .replace(/\.\./g, '_')
        .replace(/\\/g, '/')
        .replace(/\/{2,}/g, '/')
        .replace(/^\/+/, '')
        .replace(/\/+$/, '');
}

function joinDavPath(rootDir, path) {
    const root = normalizeRootDir(rootDir);
    const cleanPath = normalizeRelativePath(path);
    return [root, cleanPath].filter(Boolean).join('/');
}

function getRequestPath(request) {
    return decodeURIComponent(new URL(request.url).pathname);
}

function getStoragePathFromRequest(request, rootDir) {
    return joinDavPath(rootDir, getRequestPath(request));
}

function stripRootDirFromPath(path, rootDir) {
    const cleanPath = normalizeRelativePath(path);
    const root = normalizeRootDir(rootDir);

    if (!root) return cleanPath;
    if (cleanPath === root) return '';
    if (cleanPath.startsWith(`${root}/`)) return cleanPath.substring(root.length + 1);
    return cleanPath;
}

function encodePathSegments(path) {
    return normalizeRelativePath(path)
        .split('/')
        .map(segment => encodeURIComponent(segment))
        .join('/');
}

function toVirtualContents(contents, rootDir) {
    return {
        files: contents.files.map(file => ({
            ...file,
            name: stripRootDirFromPath(file.name, rootDir),
        })).filter(file => file.name),
        directories: contents.directories
            .map(dir => stripRootDirFromPath(dir, rootDir))
            .filter(Boolean),
    };
}

async function getApiHeaders(env) {
    const securityConfig = await fetchSecurityConfig(env);

    const adminUsername = securityConfig.auth.admin.adminUsername;
    const adminPassword = securityConfig.auth.admin.adminPassword;
    const authCode = securityConfig.auth.user.authCode;

    let credentials = btoa('unset:unset');

    if (adminUsername && adminPassword) {
        credentials = btoa(`${adminUsername}:${adminPassword}`);
    }

    return {
        'Authorization': `Basic ${credentials}`,
        'authCode': authCode || ''
    };
}

function checkAuth(request, othersConfig) {
    const enabled = othersConfig.webDAV.enabled;
    if (!enabled) return new Response('WebDAV is disabled', { status: 403 }); // WebDAV disabled

    const davUser = othersConfig.webDAV.username;
    const davPass = othersConfig.webDAV.password;
    if (!davUser || !davPass) return null; // No auth required

    const authHeader = request.headers.get('Authorization');
    if (!authHeader) {
        return new Response('Authorization required', {
            status: 401,
            headers: { 'WWW-Authenticate': 'Basic realm="WebDAV"' },
        });
    }

    const [scheme, encoded] = authHeader.split(' ');
    if (scheme !== 'Basic' || !encoded) {
        return new Response('Malformed Authorization header', { status: 400 });
    }

    const [user, pass] = atob(encoded).split(':');
    if (user !== davUser || pass !== davPass) {
        return new Response('Invalid credentials', { status: 403 });
    }

    return null;
}

// --- WEBDAV METHOD HANDLERS ---

function handleOptions(request, othersConfig) {
    return new Response(null, {
        status: 204,
        headers: {
            ...getCorsHeaders(request, othersConfig),
            'Allow': webDavMethods,
            'DAV': '1, 2',
            'MS-Author-Via': 'DAV',
            'Vary': 'Origin',
        },
    });
}

async function handleGet(request, env, rootDir) {
    const path = getRequestPath(request);
    const storagePath = getStoragePathFromRequest(request, rootDir);

    if (path.endsWith('/')) { // Directory listing
        try {
            const contents = await fetchDirectoryContents(storagePath, env, request);
            const virtualContents = toVirtualContents(contents, rootDir);
            const html = generateDirectoryListingHtml(path, virtualContents);
            if (request.method === 'HEAD') {
                return new Response(null, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
            }
            return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
        } catch (error) {
            console.error('GET (directory) failed:', error.stack);
            return new Response(`Error listing directory: ${error.message}`, { status: 500 });
        }
    } else { // File download
        try {
            const fileUrl = new URL(`/file/${encodePathSegments(storagePath)}`, request.url);

            const fileResponse = await fetch(fileUrl.toString(), { method: request.method });

            if (!fileResponse.ok) {
                 return new Response('File not found', { status: fileResponse.status, statusText: fileResponse.statusText });
            }

            const response = new Response(fileResponse.body, fileResponse);
            response.headers.set('Access-Control-Allow-Origin', '*');

            return response;
        } catch (error) {
            console.error('GET (file) failed:', error.stack);
            return new Response(`Error getting file: ${error.message}`, { status: 500 });
        }
    }
}

async function handlePut(request, env, rootDir, othersConfig) {
    const fullPath = getStoragePathFromRequest(request, rootDir);
    if (!fullPath || getRequestPath(request).endsWith('/')) {
        return new Response('Invalid file name', { status: 400 });
    }

    const lastSlashIndex = fullPath.lastIndexOf('/');
    let uploadFolder = lastSlashIndex > -1 ? fullPath.substring(0, lastSlashIndex) : '';
    const fileName = lastSlashIndex > -1 ? fullPath.substring(lastSlashIndex + 1) : fullPath;

    // 路径安全处理：防止路径穿越
    uploadFolder = normalizeRelativePath(uploadFolder);
    
    const fileContent = await request.blob();
    const formData = new FormData();
    formData.append('file', fileContent, fileName);

    const uploadUrl = new URL(`/upload`, request.url);
    if (uploadFolder) {
        uploadUrl.searchParams.set('uploadFolder', uploadFolder);
    }

    // 获取 WebDAV 配置的上传渠道
    const webdavConfig = othersConfig.webDAV || {};
    if (webdavConfig.uploadChannel) {
        uploadUrl.searchParams.set('uploadChannel', webdavConfig.uploadChannel);
    }
    if (webdavConfig.channelName) {
        uploadUrl.searchParams.set('channelName', webdavConfig.channelName);
    }

    try {
        const response = await fetch(uploadUrl.toString(), { 
            method: 'POST', 
            body: formData,
            headers: await getApiHeaders(env)
        });
        const result = await response.json(); 
        if (response.ok && Array.isArray(result) && result.length > 0 && result[0].src) {
            return new Response(null, { status: 201 }); // Created
        } else {
            const errorMsg = result.error || JSON.stringify(result);
            console.error('Upload API error:', errorMsg);
            return new Response(`Upload failed: ${errorMsg}`, { status: 500 });
        }
    } catch (error) {
        console.error('Fetch to upload API failed:', error.stack);
        return new Response('Failed to contact upload service', { status: 502 });
    }
}

async function handleDelete(request, env, rootDir) {
    const path = getStoragePathFromRequest(request, rootDir);
    if (!path) return new Response('Invalid path for DELETE', { status: 400 });

    const isFolder = getRequestPath(request).endsWith('/');
    const cleanPath = isFolder ? path.slice(0, -1) : path;
    
    const deleteUrl = new URL(`/api/manage/delete/${encodePathSegments(cleanPath)}`, request.url);
    if (isFolder) deleteUrl.searchParams.set('folder', 'true');

    try {
        const response = await fetch(deleteUrl.toString(), {
            method: 'DELETE',
            headers: await getApiHeaders(env)
        });
        const result = await response.json();
        if (result.success) {
            return new Response(null, { status: 204 }); // No Content
        } else {
            console.error('Delete API error:', JSON.stringify(result));
            return new Response(`Deletion failed: ${result.error || 'API error'}`, { status: 500 });
        }
    } catch (error) {
        console.error('Delete operation failed:', error.stack);
        return new Response(`Internal server error: ${error.message}`, { status: 500 });
    }
}

async function handlePropfind(request, env, rootDir) {
    const path = getRequestPath(request);
    const storagePath = getStoragePathFromRequest(request, rootDir);
    try {
        const contents = await fetchDirectoryContents(storagePath, env, request);
        const virtualContents = toVirtualContents(contents, rootDir);
        const xml = generateWebDAVXml(path, virtualContents);
        return new Response(xml, { status: 207, headers: { 'Content-Type': 'application/xml; charset=utf-8' } });
    } catch (error) {
        console.error('Propfind failed:', error.stack);
        return new Response(`Failed to list files: ${error.message}`, { status: 500 });
    }
}

// --- API DATA FETCHING ---

async function fetchDirectoryContents(dir, env, request) {
    let allFiles = [];
    let allDirectories = [];
    const count = -1; // Fetch all items

    const listUrl = new URL(`/api/manage/list`, request.url);
    listUrl.searchParams.set('dir', dir);
    listUrl.searchParams.set('count', count);

    const response = await fetch(listUrl.toString(), { headers: await getApiHeaders(env) });
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API fetch error: Status ${response.status} - ${errorText}`);
    }
    
    const result = await response.json();
    if (result.error) {
        throw new Error(`API error: ${result.error} - ${result.message}`);
    }

    if (result.files && result.files.length > 0) allFiles = allFiles.concat(result.files);
    if (result.directories && result.directories.length > 0) allDirectories = allDirectories.concat(result.directories);


    return { files: allFiles, directories: [...new Set(allDirectories)] };
}

// --- HTML and XML GENERATION ---

function generateDirectoryListingHtml(basePath, contents) {
    let fileLinks = '';
    let dirLinks = '';

    for (const dir of contents.directories) {
        const fullDirPath = `/dav/${dir}/`;
        const dirName = dir.split('/').pop();
        dirLinks += `<li><a href="${fullDirPath}"><strong>${dirName}/</strong></a></li>`;
    }

    for (const file of contents.files) {
        const fullFilePath = `/dav/${file.name}`; 
        const fileName = file.name.split('/').pop();
        const fileSize = file.metadata && file.metadata['FileSize'] 
            ? `${file.metadata['FileSize']} MB` 
            : 'N/A';
        fileLinks += `<li><a href="${fullFilePath}">${fileName}</a> - ${fileSize}</li>`;
    }
    
    let parentDirLink = '';
    if (basePath !== '/') {
        const parentPath = new URL('..', `http://dummy.com${basePath}`).pathname;
        parentDirLink = `<li><a href="/dav${parentPath}"><strong>../ (Parent Directory)</strong></a></li>`;
    }

    return `<!DOCTYPE html><html><head><title>Index of ${basePath}</title><meta name="viewport" content="width=device-width, initial-scale=1.0"><style>body{font-family:sans-serif;padding:20px}li{margin:5px 0}</style></head><body><h1>Index of ${basePath}</h1><ul>${parentDirLink}${dirLinks}${fileLinks}</ul></body></html>`;
}

function generateWebDAVXml(basePath, contents) {
    let responses = '';
    const currentPath = basePath.endsWith('/') ? basePath : `${basePath}/`;

    responses += createCollectionXml(currentPath);

    for (const dir of contents.directories) {
        responses += createCollectionXml(`/${dir}/`);
    }
    for (const file of contents.files) {
        responses += createFileXml(file);
    }
    return `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">${responses}</D:multistatus>`;
}

function createCollectionXml(path) {
    const now = new Date().toUTCString();
    const cleanPath = path.endsWith('/') ? path.slice(0, -1) : path;
    const name = cleanPath.split('/').pop() || '';
    return `<D:response><D:href>${encodeURI(path)}</D:href><D:propstat><D:prop><D:displayname>${name}</D:displayname><D:resourcetype><D:collection/></D:resourcetype><D:creationdate>${now}</D:creationdate><D:getlastmodified>${now}</D:getlastmodified></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`;
}

function createFileXml(file) {
    const now = new Date().toUTCString();
    const fileSize = file.metadata && file.metadata['File-Size'] ? file.metadata['File-Size'] : "0";
    return `<D:response><D:href>${encodeURI(`/${file.name}`)}</D:href><D:propstat><D:prop><D:displayname>${file.name.split('/').pop()}</D:displayname><D:resourcetype/><D:creationdate>${now}</D:creationdate><D:getlastmodified>${now}</D:getlastmodified><D:getcontentlength>${fileSize}</D:getcontentlength></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`;
}
