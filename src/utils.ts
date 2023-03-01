export function convertToAlphaNumberic(str) {
    return str.replace(/[^a-z0-9-_]/gi, '_');
}
