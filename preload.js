const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('xbound', {
  appName: '',
  loadPrecomputedEstimates: (benchmark, xboundParams) => ipcRenderer.invoke('xbound:load-precomputed-estimates', benchmark, xboundParams),
  loadWorkloadQueries: (benchmark) => ipcRenderer.invoke('xbound:load-workload-queries', benchmark),
  estimateCustomQuery: (benchmark, sql, xboundParams, queryTag) => ipcRenderer.invoke('xbound:estimate-custom-query', benchmark, sql, xboundParams, queryTag)
});
