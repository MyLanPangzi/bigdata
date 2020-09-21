const fs = require('fs');

let json = JSON.parse(fs.readFileSync('display.json', 'utf-8'));
Object.keys(json.common)
    .forEach(e => console.log(`get_json_object(line, '$.common.${e}'),`))
Object.keys(json.page)
    .forEach(e => console.log(`get_json_object(line, '$.page.${e}'),`))
Object.keys(json.displays[0])
    .forEach(e => console.log(`get_json_object(display, '$.display.${e}'),`))
console.log(`get_json_object(line, '$.ts')`)



