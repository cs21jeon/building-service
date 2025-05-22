// app.js - Main application file
require('dotenv').config({ path: '/root/goldenrabbit/.env' });
const express = require('express');
const cron = require('node-cron');
const axios = require('axios');
const Airtable = require('airtable');
const { convert } = require('xml-js');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.BUILDING_SERVICE_PORT || 3000;

// 로그 디렉토리 설정
const logDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// 로그 레벨 설정 (환경 변수에서 가져오거나 기본값 사용)
const LOG_LEVEL = process.env.LOG_LEVEL || 'info'; // 'debug', 'info', 'warn', 'error'

// 로그 레벨 우선순위 매핑
const LOG_LEVELS = {
  'debug': 0,
  'info': 1,
  'warn': 2,
  'error': 3
};

// 로그 파일에 저장하는 함수
function logToFile(level, message) {
  const now = new Date();
  const logFile = path.join(logDir, `${now.toISOString().split('T')[0]}.log`);
  const timestamp = now.toISOString();
  fs.appendFileSync(logFile, `[${timestamp}] [${level.toUpperCase()}] ${message}\n`);
}

// 로그 레벨에 따른 로깅 함수
function log(level, ...args) {
  // 현재 설정된 로그 레벨보다 낮은 메시지는 무시
  if (LOG_LEVELS[level] < LOG_LEVELS[LOG_LEVEL]) {
    return;
  }

  const message = args.join(' ');
  
  // 콘솔에 출력
  if (level === 'error') {
    console.error(`[${level.toUpperCase()}]`, message);
  } else {
    console.log(`[${level.toUpperCase()}]`, message);
  }
  
  // 파일에 저장
  logToFile(level, message);
}

// 각 로그 레벨에 대한 편의 함수
const logger = {
  debug: (...args) => log('debug', ...args),
  info: (...args) => log('info', ...args),
  warn: (...args) => log('warn', ...args),
  error: (...args) => log('error', ...args)
};

// 콘솔 로그 재정의 (필요한 경우에만 사용)
// 기존 console.log와 error 대신 logger 객체를 사용하는 것이 더 좋습니다
const originalConsoleLog = console.log;
console.log = function(...args) {
  originalConsoleLog.apply(console, args);
  // 로그 레벨이 info 이상인 경우에만 파일에 기록
  if (LOG_LEVELS[LOG_LEVEL] <= LOG_LEVELS['info']) {
    const message = args.join(' ');
    logToFile('info', message);
  }
};

const originalConsoleError = console.error;
console.error = function(...args) {
  originalConsoleError.apply(console, args);
  // 에러는 항상 기록
  const message = args.join(' ');
  logToFile('error', message);
};

// 로그 정리 함수 (7일 이상 된 로그 파일 삭제)
const cleanupLogs = () => {
  fs.readdir(logDir, (err, files) => {
    if (err) return logger.error('로그 정리 중 오류:', err);
    
    const now = new Date();
    let deletedCount = 0;
    
    files.forEach(file => {
      if (!file.endsWith('.log')) return;
      
      const filePath = path.join(logDir, file);
      const fileDate = new Date(file.split('.')[0]);
      const daysDiff = (now - fileDate) / (1000 * 60 * 60 * 24);
      
      if (daysDiff > 7) {
        fs.unlinkSync(filePath);
        deletedCount++;
      }
    });
    
    if (deletedCount > 0) {
      logger.info(`오래된 로그 파일 ${deletedCount}개 삭제 완료`);
    } else {
      logger.debug('삭제할 오래된 로그 파일이 없습니다');
    }
  });
};

// 매일 자정에 로그 정리 실행
cron.schedule('0 0 * * *', cleanupLogs);

// 에어테이블 설정 - Access Token 사용
const airtableBase = new Airtable({
  apiKey: process.env.AIRTABLE_ACCESS_TOKEN || process.env.AIRTABLE_API_KEY
}).base(process.env.AIRTABLE_BASE_ID);

// 건축물 정보용 테이블/뷰
const BUILDING_TABLE = process.env.AIRTABLE_BUILDING_TABLE;
const BUILDING_VIEW = process.env.AIRTABLE_BUILDING_VIEW;

// 토지 정보용 테이블/뷰
const LAND_TABLE = process.env.AIRTABLE_LAND_TABLE;
const LAND_VIEW = process.env.AIRTABLE_LAND_VIEW;

// 그 외 API 키들
const PUBLIC_API_KEY = process.env.PUBLIC_API_KEY;
const GOOGLE_SCRIPT_URL = process.env.GOOGLE_SCRIPT_URL;

// 공통 함수: 주소 파싱
const parseAddress = (address) => {
  if (!address || typeof address !== "string" || address.trim() === "") {
    return { error: "주소 없음", 원본주소: address || "입력값 없음" };
  }
  
  address = address.trim().replace(/\s+/g, ' ');
  
  // 정규식으로 "구/시/군 법정동 번-지" 또는 "구/시/군 법정동 번" 패턴 분리
  const match = address.match(/^(\S+구|\S+시|\S+군) (\S+) (\d+)(?:-(\d+))?$/);
  
  if (!match) {
    return { error: "잘못된 주소 형식", 원본주소: address };
  }
  
  const 시군구 = match[1];
  const 법정동 = match[2];
  const 번 = match[3].padStart(4, '0');
  const 지 = match[4] ? match[4].padStart(4, '0') : "0000";
  
  return { 시군구, 법정동, 번, 지 };
};

// 공통 함수: 구글 스크립트를 통해 코드 가져오기
const getBuildingCodes = async (addressData) => {
  try {
    console.log('주소 데이터로 건축물 코드 조회 요청:', JSON.stringify(addressData));
    
    const response = await axios.post(
      process.env.GOOGLE_SCRIPT_URL || 'https://script.google.com/macros/s/AKfycbycxM4pNuDvzZp_iTsteqxWu738wMWfpPcLbzpHYNDD3CLg7oU1sFXycQfyZcounUDPVQ/exec',
      [addressData]
    );
    
    console.log('Google 스크립트 API 응답:', JSON.stringify(response.data));
    
    // API 응답이 배열인 경우 처리
    if (Array.isArray(response.data) && response.data.length > 0) {
      const data = response.data[0];
      if (data.시군구코드 !== undefined && data.법정동코드 !== undefined) {
        // 숫자 타입을 문자열로 변환 (필요한 경우)
        const 시군구코드 = String(data.시군구코드);
        const 법정동코드 = String(data.법정동코드);
        
        return {
          ...addressData,
          시군구코드,
          법정동코드
        };
      }
    }
    // API 응답이 객체인 경우 처리
    else if (response.data && response.data.시군구코드 !== undefined && response.data.법정동코드 !== undefined) {
      // 숫자 타입을 문자열로 변환 (필요한 경우)
      const 시군구코드 = String(response.data.시군구코드);
      const 법정동코드 = String(response.data.법정동코드);
      
      return {
        ...addressData,
        시군구코드,
        법정동코드
      };
    }
    
    console.warn('API에서 코드를 찾을 수 없음, 응답:', JSON.stringify(response.data));
    throw new Error('Building codes not found in response');
  } catch (error) {
    console.error('Error getting building codes:', error);
    throw error;
  }
};

// 공통 함수: PNU 생성
const generatePNU = (codeData) => {
  if (!codeData.시군구코드 || !codeData.법정동코드 || !codeData.번 || !codeData.지) {
    return null;
  }
  return `${codeData.시군구코드}${codeData.법정동코드}1${codeData.번}${codeData.지}`;
};

// =================================
// 건축물 정보 처리 기능
// =================================

// 건축물 정보 가져오기
const getBuildingData = async (codeData) => {
  try {
    logger.debug('Fetching building data for:', codeData.id);
    
    const url = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo';
    const response = await axios.get(url, {
      params: {
        serviceKey: process.env.PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        _type: 'json',
        numOfRows: 10,
        pageNo: 1
      },
      headers: {
        accept: '*/*'
      }
    });
    
    logger.debug('Building data API response received');
    return {
      ...response.data,
      id: codeData.id
    };
  } catch (error) {
    logger.error('Error fetching building data from public API:', error.message);
    // 응답 상태 코드가 있으면 로깅
    if (error.response) {
      logger.error('API response status:', error.response.status);
      logger.debug('API response data:', JSON.stringify(error.response.data));
    }
    return { body: {}, id: codeData.id };
  }
};

// 건축물 아이템 추출 및 처리
const extractBuildingItems = (data) => {
  try {
    if (!data || !data.response || !data.response.body || !data.response.body.items) {
      return [];
    }
    
    const itemArray = data.response.body.items.item;
    
    if (!itemArray || !Array.isArray(itemArray) || itemArray.length === 0) {
      return [];
    }
    
    return itemArray.map(item => {
      if (item.platPlc) {
        item.platPlc = item.platPlc.replace(/^\S+\s/, '').replace(/번지$/, '');
      }
      return item;
    });
  } catch (error) {
    console.error('Error extracting building items:', error);
    return [];
  }
};

// 건축물 데이터 처리
const processBuildingData = (item) => {
  const data = { ...item };
  
  // 날짜 변환
  const dateFields = ["crtnDay", "useAprDay"];
  const formatDateISO = (dateStr) => {
    if (!dateStr || dateStr.length !== 8 || dateStr === "00000000") return dateStr;
    const formattedDate = `${dateStr.substring(0, 4)}-${dateStr.substring(4, 6)}-${dateStr.substring(6, 8)}`;
    
    const date = new Date(`${formattedDate}T00:00:00.000Z`);
    return isNaN(date.getTime()) ? dateStr : date.toISOString();
  };
  
  dateFields.forEach(field => {
    if (data[field]) data[field] = formatDateISO(data[field]);
  });
  
  // 승강기수 계산
  const 승강기수 = (parseInt(data["rideUseElvtCnt"], 10) || 0) + (parseInt(data["emgenUseElvtCnt"], 10) || 0);
  
  // 주차대수 계산
  const 주차대수 = (parseInt(data["indrMechUtcnt"], 10) || 0) + 
                 (parseInt(data["oudrMechUtcnt"], 10) || 0) + 
                 (parseInt(data["indrAutoUtcnt"], 10) || 0) + 
                 (parseInt(data["oudrAutoUtcnt"], 10) || 0);
  
  data["승강기수"] = 승강기수;
  data["주차대수"] = 주차대수;
  
  // "세대/가구/호" 변환
  const 세대 = parseInt(data["hhldCnt"], 10) || 0;
  const 가구 = parseInt(data["fmlyCnt"], 10) || 0;
  const 호 = parseInt(data["hoCnt"], 10) || 0;
  data["세대/가구/호"] = `${세대}/${가구}/${호}`;
  
  // "층수" 변환
  const 지상층수 = parseInt(data["grndFlrCnt"], 10) || 0;
  const 지하층수 = parseInt(data["ugrndFlrCnt"], 10) || 0;
  data["층수"] = `-${지하층수}/${지상층수}`;
  
  return data;
};

// 건축물 필드명 매핑
const mapBuildingFieldNames = (item) => {
  const keyMap = {
    "rnum": "순번",
    "platPlc": "지번 주소",
    "sigunguCd": "시군구코드",
    "bjdongCd": "법정동코드",
    "bun": "번",
    "ji": "지",
    "mainPurpsCdNm": "주용도",
    "etcPurps": "기타용도",
    "roofCdNm": "지붕",
    "heit": "높이(m)",
    "useAprDay": "사용승인일",
    "crtnDay": "생성일자",
    "newPlatPlc": "도로명주소",
    "platGbCd": "대지",
    "bldNm": "건물명",
    "platArea": "대지면적(㎡)",
    "archArea": "건축면적(㎡)",
    "bcRat": "건폐율(%)",
    "totArea": "연면적(㎡)",
    "vlRatEstmTotArea": "용적률산정용연면적(㎡)",
    "vlRat": "용적률(%)",
    "strctCdNm": "주구조"
  };

  let newItem = {};
  for (const oldKey in item) {
    const newKey = keyMap[oldKey] || oldKey;
    newItem[newKey] = item[oldKey];
  }
  
  return newItem;
};

// 에어테이블 건축물 정보 업데이트
const updateBuildingInfo = async (buildingData, recordId) => {
  try {
    const updateData = {
      "대지면적(㎡)": buildingData["대지면적(㎡)"],
      "연면적(㎡)": buildingData["연면적(㎡)"],
      "용적률산정용연면적(㎡)": buildingData["용적률산정용연면적(㎡)"],
      "건축면적(㎡)": buildingData["건축면적(㎡)"],
      "건폐율(%)": buildingData["건폐율(%)"],
      "용적률(%)": buildingData["용적률(%)"],
      "높이(m)": buildingData["높이(m)"],
      "주차대수": buildingData["주차대수"],
      "승강기수": buildingData["승강기수"],
      "도로명주소": buildingData["도로명주소"],
      "생성일자": buildingData["생성일자"],
      "사용승인일": buildingData["사용승인일"] && buildingData["사용승인일"].trim() !== '' 
                  ? buildingData["사용승인일"] 
                  : undefined,
      "층수": buildingData["층수"],
      "기타용도": buildingData["기타용도"],
      "주용도": buildingData["주용도"],
      "지붕": buildingData["지붕"],
      "주구조": buildingData["주구조"],
      "건물명": buildingData["건물명"],
      "세대/가구/호": buildingData["세대/가구/호"]
    };
    
    await airtableBase(process.env.AIRTABLE_BUILDING_TABLE).update(recordId, updateData);
    console.log(`Updated Airtable building record ${recordId}`);
    return true;
  } catch (error) {
    console.error(`Error updating Airtable building record ${recordId}:`, error);
    return false;
  }
};

// 건축물 레코드 처리
const processBuildingRecord = async (record) => {
  try {
    // 주소 파싱
    const parsedAddress = parseAddress(record['지번 주소']);
    parsedAddress.id = record.id;
    
    if (parsedAddress.error) {
      console.log(`Address error for building record ${record.id}: ${parsedAddress.error}`);
      return false;
    }
    
    // 건축물 코드 조회
    const buildingCodes = await getBuildingCodes(parsedAddress);
    
    // 건축물 데이터 조회
    const buildingData = await getBuildingData(buildingCodes);
    
    // 데이터 추출 및 처리
    const extractedItems = extractBuildingItems(buildingData);
    
    if (extractedItems.length === 0) {
      console.log(`No building data found for record ${record.id}`);
      return false;
    }
    
    // 데이터 가공
    const processedData = processBuildingData(extractedItems[0]);
    
    // 필드명 매핑
    const mappedData = mapBuildingFieldNames(processedData);
    
    // 에어테이블 업데이트
    const updated = await updateBuildingInfo(mappedData, record.id);
    
    return updated;
  } catch (error) {
    console.error(`Error processing building record ${record.id}:`, error);
    return false;
  }
};

// =================================
// 토지 정보 처리 기능
// =================================

// VWorld API를 사용한 토지 정보 조회
const getLandData = async (pnu) => {
  try {
    console.log('VWorld API로 토지 정보 조회 요청 - PNU:', pnu);
    
    const HttpUrl = "http://api.vworld.kr/ned/data/getLandCharacteristics";
    
    // 전년도만 사용 (현재 2025년이므로 2024년)
    const currentYear = new Date().getFullYear();
    const lastYear = (currentYear - 1).toString();
    
    const params = new URLSearchParams({
      key: process.env.VWORLD_APIKEY,
      domain: 'localhost',
      pnu: pnu,
      stdrYear: lastYear,
      format: 'xml', // XML 형식으로 요청
      numOfRows: '10',
      pageNo: '1'
    });
    
    const fullUrl = `${HttpUrl}?${params.toString()}`;
    console.log(`VWorld API 요청 (${lastYear}년):`, fullUrl);
    
    const response = await axios.get(fullUrl);
    console.log(`VWorld API XML 응답 (${lastYear}년):`, response.data);
    
    // XML을 JSON으로 변환
    const jsonData = convert.xml2js(response.data, {
      compact: true,
      spaces: 2,
      textKey: '_text'
    });
    
    console.log(`변환된 JSON 데이터:`, JSON.stringify(jsonData, null, 2));
    
    // 응답 구조 확인: response.fields.field
    if (jsonData && jsonData.response && jsonData.response.fields && jsonData.response.fields.field) {
      const totalCount = parseInt(jsonData.response.totalCount._text || '0');
      if (totalCount > 0) {
        console.log(`${lastYear}년 데이터 발견! (총 ${totalCount}건)`);
        return jsonData;
      }
    }
    
    throw new Error(`${lastYear}년 토지 데이터를 찾을 수 없음`);
    
  } catch (error) {
    console.error('Error fetching land data from VWorld:', error);
    throw error;
  }
};

// VWorld API 응답에서 토지 데이터 추출
const extractLandItems = (jsonData) => {
  try {
    console.log('VWorld API 응답 데이터 추출 시작');
    
    // XML을 JSON으로 변환한 데이터에서 추출
    if (!jsonData || !jsonData.response || !jsonData.response.fields || !jsonData.response.fields.field) {
      console.log('VWorld API 응답에서 response.fields.field를 찾을 수 없음');
      return null;
    }
    
    let fieldData = jsonData.response.fields.field;
    
    // field가 배열이 아닌 경우 배열로 변환
    if (!Array.isArray(fieldData)) {
      fieldData = [fieldData];
    }
    
    if (fieldData.length === 0) {
      console.log('VWorld API 응답에서 field 데이터가 비어있음');
      return null;
    }
    
    // 가장 최신 데이터 선택 (lastUpdtDt 기준)
    const latestField = fieldData.reduce((latest, current) => {
      const currentDate = new Date(current.lastUpdtDt._text);
      const latestDate = new Date(latest.lastUpdtDt._text);
      return currentDate > latestDate ? current : latest;
    });
    
    console.log('추출된 토지 데이터:', JSON.stringify(latestField, null, 2));
    
    // XML에서 변환된 데이터는 _text 속성에 실제 값이 들어있음
    const extractedData = {
      pnu: latestField.pnu._text,
      ldCode: latestField.ldCode._text,
      ldCodeNm: latestField.ldCodeNm._text,
      regstrSeCode: latestField.regstrSeCode._text,
      regstrSeCodeNm: latestField.regstrSeCodeNm._text,
      mnnmSlno: latestField.mnnmSlno._text,
      ladSn: latestField.ladSn._text,
      stdrYear: latestField.stdrYear._text,
      stdrMt: latestField.stdrMt._text,
      lndcgrCode: latestField.lndcgrCode._text,
      lndcgrCodeNm: latestField.lndcgrCodeNm._text,
      lndpclAr: latestField.lndpclAr._text,
      prposArea1: latestField.prposArea1._text,
      prposArea1Nm: latestField.prposArea1Nm._text,
      prposArea2: latestField.prposArea2._text,
      prposArea2Nm: latestField.prposArea2Nm._text,
      ladUseSittn: latestField.ladUseSittn._text,
      ladUseSittnNm: latestField.ladUseSittnNm._text,
      tpgrphHgCode: latestField.tpgrphHgCode._text,
      tpgrphHgCodeNm: latestField.tpgrphHgCodeNm._text,
      tpgrphFrmCode: latestField.tpgrphFrmCode._text,
      tpgrphFrmCodeNm: latestField.tpgrphFrmCodeNm._text,
      roadSideCode: latestField.roadSideCode._text,
      roadSideCodeNm: latestField.roadSideCodeNm._text,
      pblntfPclnd: latestField.pblntfPclnd._text,
      lastUpdtDt: latestField.lastUpdtDt._text
    };
    
    console.log('_text 속성 제거 후 데이터:', JSON.stringify(extractedData, null, 2));
    return extractedData;
    
  } catch (error) {
    console.error('Error extracting VWorld land items:', error);
    return null;
  }
};

// 토지 데이터 가공
const processLandData = (data) => {
  try {
    console.log('토지 데이터 가공 시작:', JSON.stringify(data, null, 2));
    
    // 주소에서 구/동 정보 추출
    const addressParts = data.ldCodeNm.split(" ");
    let selectedRegion = "";
    
    // 마지막 요소 (동/리/로/가)
    const lastElement = addressParts[addressParts.length - 1];
    
    // 주소 패턴에 따른 추출
    if (addressParts.some(part => part.includes("특별시")) || addressParts.some(part => part.includes("광역시"))) {
      const guIndex = addressParts.findIndex(part => part.endsWith("구"));
      const gunIndex = addressParts.findIndex(part => part.endsWith("군"));
      
      if (guIndex !== -1) {
        selectedRegion = `${addressParts[guIndex]} ${lastElement}`;
      } else if (gunIndex !== -1) {
        selectedRegion = `${addressParts[gunIndex]} ${lastElement}`;
      }
    } else {
      const siIndex = addressParts.findIndex(part => part.endsWith("시"));
      const gunIndex = addressParts.findIndex(part => part.endsWith("군"));
      const guIndex = addressParts.findIndex(part => part.endsWith("구"));
      
      if (siIndex !== -1 && guIndex !== -1) {
        selectedRegion = `${addressParts[guIndex]} ${lastElement}`;
      } else if (siIndex !== -1 && guIndex === -1) {
        selectedRegion = `${addressParts[siIndex]} ${lastElement}`;
      } else if (gunIndex !== -1) {
        selectedRegion = `${addressParts[gunIndex]} ${lastElement}`;
      }
    }
    
    // 숫자 데이터 타입 변환 및 검증
    const parseNumber = (value) => {
      if (!value || value === '') return null;
      const parsed = parseFloat(value);
      return isNaN(parsed) ? null : parsed;
    };
    
    const result = {
      "지번 주소": `${selectedRegion} ${data.mnnmSlno}`,
      "토지면적(㎡)": parseNumber(data.lndpclAr),
      "용도지역": data.prposArea1Nm || null,
      "공시지가(원/㎡)": parseNumber(data.pblntfPclnd),
      "토지정보업데이트": new Date(data.lastUpdtDt).toISOString()
    };
    
    console.log('가공된 토지 데이터:', JSON.stringify(result, null, 2));
    return result;
  } catch (error) {
    console.error('Error processing land data:', error);
    return null;
  }
};

// 에어테이블 토지 정보 업데이트
const updateLandInfo = async (landData, recordId) => {
  try {
    // 유효한 데이터만 업데이트 객체에 포함
    const updateData = {};
    
    if (landData["토지면적(㎡)"] !== null && landData["토지면적(㎡)"] !== undefined) {
      updateData["토지면적(㎡)"] = landData["토지면적(㎡)"];
    }
    
    if (landData["공시지가(원/㎡)"] !== null && landData["공시지가(원/㎡)"] !== undefined) {
      updateData["공시지가(원/㎡)"] = landData["공시지가(원/㎡)"];
    }
    
    if (landData["용도지역"] && landData["용도지역"].trim() !== '') {
      updateData["용도지역"] = landData["용도지역"];
    }
    
    console.log(`업데이트할 토지 데이터 (레코드 ${recordId}):`, JSON.stringify(updateData, null, 2));
    
    // 업데이트할 데이터가 있는지 확인
    if (Object.keys(updateData).length === 0) {
      console.log(`업데이트할 유효한 데이터가 없음 (레코드 ${recordId})`);
      return false;
    }
    
    await airtableBase(process.env.AIRTABLE_LAND_TABLE).update(recordId, updateData);
    console.log(`Updated Airtable land record ${recordId}`);
    return true;
  } catch (error) {
    console.error(`Error updating Airtable land record ${recordId}:`, error);
    return false;
  }
};

// 토지 레코드 처리
const processLandRecord = async (record) => {
  try {
    console.log(`Processing land record ${record.id}: ${record['지번 주소']}`);
    
    // 주소 파싱
    const parsedAddress = parseAddress(record['지번 주소']);
    parsedAddress.id = record.id;
    
    if (parsedAddress.error) {
      console.log(`Address error for land record ${record.id}: ${parsedAddress.error}`);
      return false;
    }
    
    // 코드 조회
    const codes = await getBuildingCodes(parsedAddress);
    
    // PNU 생성
    const pnu = generatePNU(codes);
    if (!pnu) {
      console.log(`Invalid PNU data for record ${record.id}`);
      return false;
    }
    
    // 토지 데이터 조회
    const landData = await getLandData(pnu);
    if (!landData) {
      console.log(`No land data found for record ${record.id}`);
      return false;
    }
    
    // 데이터 추출
    const extractedItem = extractLandItems(landData);
    if (!extractedItem) {
      console.log(`Failed to extract land data for record ${record.id}`);
      return false;
    }
    
    // 데이터 가공
    const processedData = processLandData(extractedItem);
    if (!processedData) {
      console.log(`Failed to process land data for record ${record.id}`);
      return false;
    }
    
    // 에어테이블 업데이트
    const updated = await updateLandInfo(processedData, record.id);
    
    return updated;
  } catch (error) {
    console.error(`Error processing land record ${record.id}:`, error);
    return false;
  }
};

// =================================
// 공통 작업 실행 기능
// =================================

// 건축물 정보 작업 실행 (약 620~660번 줄)
const runBuildingJob = async () => {
  try {
    console.log('Starting building information job...');
    
    // 에어테이블에서 건축물 레코드 가져오기
    const records = await airtableBase(process.env.AIRTABLE_BUILDING_TABLE)
      .select({
        view: process.env.AIRTABLE_BUILDING_VIEW // 건축물용 뷰
      })
      .all();
    
    console.log(`Found ${records.length} building records to process`);
    
    // 레코드 정보 추출
    const recordData = records.map(record => ({
      id: record.id,
      '지번 주소': record.get('지번 주소') || '',
    }));
    
    // 각 레코드 처리
    let successCount = 0;
    for (const record of recordData) {
      const success = await processBuildingRecord(record);
      if (success) successCount++;
      
      // API 요청 사이 간격
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    console.log(`Processed ${records.length} building records. ${successCount} successful, ${records.length - successCount} failed.`);
    return { total: records.length, success: successCount };
  } catch (error) {
    console.error('Error running building job:', error);
    return { total: 0, success: 0, error: error.message };
  }
};

// 토지 정보 작업 실행
const runLandJob = async () => {
  try {
    console.log('Starting land information job...');
    
    // 에어테이블에서 토지 레코드 가져오기
    const records = await airtableBase(process.env.AIRTABLE_LAND_TABLE)
      .select({
        view: process.env.AIRTABLE_LAND_VIEW
      })
      .all();
    
    console.log(`Found ${records.length} land records to process`);
    
    // 레코드 정보 추출
    const recordData = records.map(record => ({
      id: record.id,
      '지번 주소': record.get('지번 주소') || '',
    }));
    
    // 각 레코드 처리
    let successCount = 0;
    for (const record of recordData) {
      const success = await processLandRecord(record);
      if (success) successCount++;
      
      // API 요청 사이 간격
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    console.log(`Processed ${records.length} land records. ${successCount} successful, ${records.length - successCount} failed.`);
    return { total: records.length, success: successCount };
  } catch (error) {
    console.error('Error running land job:', error);
    return { total: 0, success: 0, error: error.message };
  }
};

// 변경된 코드
const runAllJobs = async () => {
  logger.info('Starting all jobs');
  
  try {
    const buildingResult = await runBuildingJob();
    logger.info('Building job completed:', JSON.stringify(buildingResult));
    
    const landResult = await runLandJob();
    logger.info('Land job completed:', JSON.stringify(landResult));
    
    const result = {
      building: buildingResult,
      land: landResult,
      timestamp: new Date().toISOString()
    };
    
    logger.info('All jobs completed successfully');
    return result;
  } catch (error) {
    logger.error('Error in runAllJobs:', error.message);
    throw error; // 오류를 다시 던져서 호출자가 처리할 수 있게 함
  }
};

// =================================
// 스케줄링 및 API 엔드포인트
// =================================

// 작업 스케줄링 (매시간 실행, 10AM-7PM)
// 매분 실행 작업
cron.schedule('* * * * *', async () => {
  logger.debug('작업 확인 중...');
  
  try {
    // 처리할 레코드가 있는지 확인
    const buildingRecords = await airtableBase(process.env.AIRTABLE_BUILDING_TABLE)
      .select({
        view: process.env.AIRTABLE_BUILDING_VIEW,
        maxRecords: 1
      })
      .all();
    
    const landRecords = await airtableBase(process.env.AIRTABLE_LAND_TABLE)
      .select({
        view: process.env.AIRTABLE_LAND_VIEW,
        maxRecords: 1
      })
      .all();
    
    // 처리할 레코드가 있는 경우에만 작업 실행
    if (buildingRecords.length > 0 || landRecords.length > 0) {
      logger.info('처리할 레코드 발견, 작업 실행 중...');
      await runAllJobs();
    } else {
      // 처리할 데이터가 없을 때는 debug 레벨로만 로깅
      logger.debug('처리할 레코드 없음, 작업 건너뜀');
    }
  } catch (error) {
    logger.error('작업 확인 중 오류 발생:', error.message);
  }
});

// 미들웨어 설정
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 정적 파일 제공
app.use(express.static(path.join(__dirname, 'public')));

// 상태 확인 엔드포인트
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

// 건축물 작업 수동 실행 엔드포인트
app.get('/run-building-job', async (req, res) => {
  try {
    console.log('Manually triggered building job at', new Date().toISOString());
    const result = await runBuildingJob();
    res.status(200).json({ message: 'Building job completed', result });
  } catch (error) {
    console.error('Error running manual building job:', error);
    res.status(500).json({ error: 'Failed to run building job', details: error.message });
  }
});

// 토지 작업 수동 실행 엔드포인트
app.get('/run-land-job', async (req, res) => {
  try {
    console.log('Manually triggered land job at', new Date().toISOString());
    const result = await runLandJob();
    res.status(200).json({ message: 'Land job completed', result });
  } catch (error) {
    console.error('Error running manual land job:', error);
    res.status(500).json({ error: 'Failed to run land job', details: error.message });
  }
});

// 모든 작업 수동 실행 엔드포인트
app.get('/run-all-jobs', async (req, res) => {
  try {
    console.log('Manually triggered all jobs at', new Date().toISOString());
    const result = await runAllJobs();
    res.status(200).json({ message: 'All jobs completed', result });
  } catch (error) {
    console.error('Error running all jobs:', error);
    res.status(500).json({ error: 'Failed to run all jobs', details: error.message });
  }
});

// 간단한 웹 인터페이스 제공
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 서버 시작
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Visit http://localhost:${PORT} to manage the service`);
});

module.exports = app;