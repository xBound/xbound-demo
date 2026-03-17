const { contextBridge } = require('electron');

contextBridge.exposeInMainWorld('xbound', {
  appName: 'xBound VLDB26 Demo'
});
