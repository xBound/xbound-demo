const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('xbound', {
  appName: 'xBound VLDB26 Demo',
  loadPrecomputedEstimates: (benchmark) => ipcRenderer.invoke('xbound:load-precomputed-estimates', benchmark),
  loadWorkloadQueries: (benchmark) => ipcRenderer.invoke('xbound:load-workload-queries', benchmark),
  estimateCustomQuery: (benchmark, sql) => ipcRenderer.invoke('xbound:estimate-custom-query', benchmark, sql)
});
