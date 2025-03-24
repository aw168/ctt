const { ChartJSNodeCanvas } = require('chartjs-node-canvas');
const fs = require('fs');

async function fetchStargazers() {
  const token = process.env.GITHUB_TOKEN;
  const response = await fetch('https://api.github.com/repos/iawooo/ctt/stargazers?per_page=100', {
    headers: {
      'Authorization': `token ${token}`,
      'Accept': 'application/vnd.github.v3.star+json',
      'User-Agent': 'CFTeleTrans'
    }
  });
  return await response.json();
}

async function generateChart() {
  const stargazers = await fetchStargazers();
  const now = new Date();
  const starCounts = Array(7).fill(0);
  const labels = [];

  for (let i = 6; i >= 0; i--) {
    const date = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const monthStr = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}`;
    labels.push(monthStr);
    const count = stargazers.filter(star => {
      const starDate = new Date(star.starred_at);
      return starDate.getFullYear() === date.getFullYear() && starDate.getMonth() === date.getMonth();
    }).length;
    starCounts[i] = count;
  }

  for (let i = 1; i < starCounts.length; i++) {
    starCounts[i] += starCounts[i - 1];
  }

  const width = 800;
  const height = 400;
  const chartJSNodeCanvas = new ChartJSNodeCanvas({ width, height });

  const configuration = {
    type: 'line',
    data: {
      labels: labels,
      datasets: [{
        label: 'Star 数量',
        data: starCounts,
        borderColor: 'rgba(75, 192, 192, 1)',
        fill: false
      }]
    },
    options: {
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: 'Star 数量' }
        },
        x: {
          title: { display: true, text: '月份' }
        }
      }
    }
  };

  const image = await chartJSNodeCanvas.renderToBuffer(configuration);
  fs.writeFileSync('images/star-chart.png', image);
}

generateChart().catch(console.error);
