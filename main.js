const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');

let win;
const activeChartWindows = {};

function createWindow() {
  win = new BrowserWindow({ 
    width: 1920, 
    height: 1080,
    icon: path.join(__dirname, 'assets/icon.png'), 
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });

  win.loadFile('index.html'); 
}

ipcMain.on('open-chart-window', (event, symbol) => {
  const upperSymbol = symbol.toUpperCase();

  if (activeChartWindows[upperSymbol]) {
    activeChartWindows[upperSymbol].focus();
    return;
  }

  let chartWindow = new BrowserWindow({
    width: 1200, 
    height: 800,
    title: `Live Chart - ${upperSymbol}`,
    icon: path.join(__dirname, 'assets/icon.png'), 
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      partition: `persist:chart-${upperSymbol}` 
    }
  });

  activeChartWindows[upperSymbol] = chartWindow;
  
  const chartUrl = `https://www.tradingview.com/chart/?symbol=NSE:${upperSymbol}&theme=light`;
  chartWindow.loadURL(chartUrl);

  chartWindow.on('close', () => {
    delete activeChartWindows[upperSymbol];
  });
});

ipcMain.on('close-all-charts', () => {
  Object.keys(activeChartWindows).forEach((symbol) => {
    if (activeChartWindows[symbol] && !activeChartWindows[symbol].isDestroyed()) {
      activeChartWindows[symbol].close();
    }
  });
});

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});