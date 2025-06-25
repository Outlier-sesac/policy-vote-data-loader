const axios = require('axios');
const fs = require('fs').promises;

const APIs = [
  {
    name: 'ALLNAMEMBER',
    url: 'https://open.assembly.go.kr/portal/openapi/ALLNAMEMBER',
    filename: 'assembly_members_integrated.json'
  },
  {
    name: 'nprlapfmaufmqytet',
    url: 'https://open.assembly.go.kr/portal/openapi/nprlapfmaufmqytet',
    filename: 'assembly_members_history_daesu_{DAESU}.json',
    isDaesuIteration: true
  },
  {
    name: 'nwvrqwxyaytdsfvhu',
    url: 'https://open.assembly.go.kr/portal/openapi/nwvrqwxyaytdsfvhu',
    filename: 'assembly_members_profile.json'
  },
  {
    name: 'nzmimeepazxkubdpn',
    url: 'https://open.assembly.go.kr/portal/openapi/nzmimeepazxkubdpn',
    filename: 'assembly_bills_age_{AGE}.json',
    isAgeIteration: true
  }
];

const API_KEY = '84d2bb829a0d413b97da6e9d2809db9e';
const PAGE_SIZE = 1000;

async function fetchAPIData(api) {
  console.log(`Starting data collection for ${api.name}...`);
  
  let allData = [];
  let pIndex = api.startIndex || 1;
  let hasMoreData = true;

  while (hasMoreData) {
    try {
      const url = `${api.url}?KEY=${API_KEY}&Type=json&pIndex=${pIndex}&pSize=${PAGE_SIZE}${api.extraParams || ''}`;
      console.log(`Fetching ${api.name} - Page ${pIndex}...`);
      
      const response = await axios.get(url);
      const data = response.data;
      
      // Extract the actual data array from the response
      const dataKey = Object.keys(data)[0];
      const items = data[dataKey]?.[1]?.row || [];
      
      if (items.length === 0) {
        hasMoreData = false;
        console.log(`No more data for ${api.name} at page ${pIndex}`);
      } else {
        allData = allData.concat(items);
        console.log(`Collected ${items.length} items from ${api.name} page ${pIndex}`);
        pIndex++;
      }
      
      // Add delay to avoid overwhelming the server
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`Error fetching ${api.name} page ${pIndex}:`, error.message);
      hasMoreData = false;
    }
  }

  return {
    api: api.name,
    totalItems: allData.length,
    data: allData
  };
}

async function fetchDaesuData(api, daesu) {
  console.log(`Starting data collection for ${api.name} DAESU ${daesu}...`);
  
  let allData = [];
  let pIndex = 1;
  let hasMoreData = true;

  while (hasMoreData) {
    try {
      const url = `${api.url}?KEY=${API_KEY}&Type=json&pIndex=${pIndex}&pSize=${PAGE_SIZE}&DAESU=${daesu}`;
      console.log(`Fetching ${api.name} DAESU ${daesu} - Page ${pIndex}...`);
      
      const response = await axios.get(url);
      const data = response.data;
      
      // Extract the actual data array from the response
      const dataKey = Object.keys(data)[0];
      const items = data[dataKey]?.[1]?.row || [];
      
      if (items.length === 0) {
        hasMoreData = false;
        console.log(`No more data for ${api.name} DAESU ${daesu} at page ${pIndex}`);
      } else {
        allData = allData.concat(items);
        console.log(`Collected ${items.length} items from ${api.name} DAESU ${daesu} page ${pIndex}`);
        pIndex++;
      }
      
      // Add delay to avoid overwhelming the server
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`Error fetching ${api.name} DAESU ${daesu} page ${pIndex}:`, error.message);
      hasMoreData = false;
    }
  }

  return {
    api: api.name,
    daesu: daesu,
    totalItems: allData.length,
    data: allData
  };
}

async function fetchAgeData(api, age) {
  console.log(`Starting data collection for ${api.name} AGE ${age}...`);
  
  let allData = [];
  let pIndex = api.startIndex || 1;
  let hasMoreData = true;

  while (hasMoreData) {
    try {
      const url = `${api.url}?KEY=${API_KEY}&Type=json&pIndex=${pIndex}&pSize=${PAGE_SIZE}&AGE=${age}`;
      console.log(`Fetching ${api.name} AGE ${age} - Page ${pIndex}...`);
      
      const response = await axios.get(url);
      const data = response.data;
      
      // Extract the actual data array from the response
      const dataKey = Object.keys(data)[0];
      const items = data[dataKey]?.[1]?.row || [];
      
      if (items.length === 0) {
        hasMoreData = false;
        console.log(`No more data for ${api.name} AGE ${age} at page ${pIndex}`);
      } else {
        allData = allData.concat(items);
        console.log(`Collected ${items.length} items from ${api.name} AGE ${age} page ${pIndex}`);
        pIndex++;
      }
      
      // Add delay to avoid overwhelming the server
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`Error fetching ${api.name} AGE ${age} page ${pIndex}:`, error.message);
      hasMoreData = false;
    }
  }

  return {
    api: api.name,
    age: age,
    totalItems: allData.length,
    data: allData
  };
}

async function saveToFile(result, filename) {
  try {
    await fs.writeFile(filename, JSON.stringify(result, null, 2), 'utf8');
    console.log(`Saved ${result.totalItems} items to ${filename}`);
  } catch (error) {
    console.error(`Error saving to ${filename}:`, error.message);
  }
}

async function main() {
  console.log('Starting API data aggregation...');
  
  for (const api of APIs) {
    try {
      if (api.isDaesuIteration) {
        // Handle DAESU iteration for nprlapfmaufmqytet API
        for (let daesu = 10; daesu <= 22; daesu++) {
          const result = await fetchDaesuData(api, daesu);
          const filename = api.filename.replace('{DAESU}', daesu);
          await saveToFile(result, filename);
          console.log(`Completed ${api.name} DAESU ${daesu}: ${result.totalItems} total items\n`);
        }
      } else if (api.isAgeIteration) {
        // Handle AGE iteration for nzmimeepazxkubdpn API
        for (let age = 10; age <= 22; age++) {
          const result = await fetchAgeData(api, age);
          const filename = api.filename.replace('{AGE}', age);
          await saveToFile(result, filename);
          console.log(`Completed ${api.name} AGE ${age}: ${result.totalItems} total items\n`);
        }
      } else {
        const result = await fetchAPIData(api);
        await saveToFile(result, api.filename);
        console.log(`Completed ${api.name}: ${result.totalItems} total items\n`);
      }
    } catch (error) {
      console.error(`Failed to process ${api.name}:`, error.message);
    }
  }
  
  console.log('All APIs processed successfully!');
}

main().catch(console.error);
