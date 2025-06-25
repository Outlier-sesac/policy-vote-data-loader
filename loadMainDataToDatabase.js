require('dotenv').config();
const sql = require('mssql');
const fs = require('fs').promises;
const path = require('path');

// Database configuration
const config = {
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  options: {
    encrypt: true,
    trustServerCertificate: false
  }
};

// Table schemas - 실제 JSON 데이터 구조에 맞게 수정
const schemas = {
  assembly_bills: `
    CREATE TABLE assembly_bills (
      id INT IDENTITY(1,1) PRIMARY KEY,
      BILL_ID NVARCHAR(50),
      BILL_NO NVARCHAR(50),
      BILL_NAME NVARCHAR(500),
      COMMITTEE NVARCHAR(200),
      PROPOSE_DT DATE,
      PROC_RESULT NVARCHAR(100),
      AGE NVARCHAR(10),
      DETAIL_LINK NVARCHAR(1000),
      PROPOSER NVARCHAR(500),
      MEMBER_LIST NVARCHAR(1000),
      LAW_PROC_DT DATE,
      LAW_PRESENT_DT DATE,
      LAW_SUBMIT_DT DATE,
      CMT_PROC_RESULT_CD NVARCHAR(100),
      CMT_PROC_DT DATE,
      CMT_PRESENT_DT DATE,
      COMMITTEE_DT DATE,
      PROC_DT DATE,
      COMMITTEE_ID NVARCHAR(50),
      PUBL_PROPOSER NVARCHAR(MAX),
      LAW_PROC_RESULT_CD NVARCHAR(100),
      RST_PROPOSER NVARCHAR(200),
      age_number INT,
      created_at DATETIME2 DEFAULT GETDATE()
    )
  `,
  assembly_members_history: `
    CREATE TABLE assembly_members_history (
      id INT IDENTITY(1,1) PRIMARY KEY,
      MONA_CD NVARCHAR(50),
      HG_NM NVARCHAR(100),
      HJ_NM NVARCHAR(100),
      ENG_NM NVARCHAR(200),
      BTH_GBN_NM NVARCHAR(50),
      BTH_DATE NVARCHAR(20),
      AGED NVARCHAR(10),
      JOB_RES_NM NVARCHAR(200),
      POLY_NM NVARCHAR(100),
      ORIG_NM NVARCHAR(100),
      ELECT_GBN_NM NVARCHAR(100),
      CMIT_NM NVARCHAR(200),
      REELE_GBN_NM NVARCHAR(100),
      UNITS NVARCHAR(100),
      SEX_GBN_NM NVARCHAR(20),
      TEL_NO NVARCHAR(50),
      E_MAIL NVARCHAR(100),
      HOMEPAGE NVARCHAR(200),
      STAFF NVARCHAR(500),
      SECRETARY NVARCHAR(200),
      SECRETARY2 NVARCHAR(200),
      ASSEM_ADDR NVARCHAR(300),
      MEM_TITLE NTEXT,
      DAESU INT,
      created_at DATETIME2 DEFAULT GETDATE()
    )
  `,
  // 추후 suffix _daesu 지우기
  assembly_members_history_daesu: `
    CREATE TABLE assembly_members_history_daesu (
      id INT IDENTITY(1,1) PRIMARY KEY,
      DAESU NVARCHAR(10),
      DAE NTEXT,
      DAE_NM NVARCHAR(100),
      NAME NVARCHAR(100),
      NAME_HAN NVARCHAR(100),
      JA NVARCHAR(100),
      HO NTEXT,
      BIRTH NVARCHAR(50),
      BON NVARCHAR(100),
      POSI NVARCHAR(200),
      HAK NTEXT,
      HOBBY NVARCHAR(500),
      BOOK NTEXT,
      SANG NTEXT,
      DEAD NVARCHAR(50),
      URL NVARCHAR(500),
      created_at DATETIME2 DEFAULT GETDATE()
    )
  `,
  assembly_members_integrated: `
    CREATE TABLE assembly_members_integrated (
      id INT IDENTITY(1,1) PRIMARY KEY,
      NAAS_CD NVARCHAR(50),
      NAAS_NM NVARCHAR(100),
      NAAS_CH_NM NVARCHAR(100),
      NAAS_EN_NM NVARCHAR(200),
      BIRDY_DIV_CD NVARCHAR(10),
      BIRDY_DT NVARCHAR(20),
      DTY_NM NVARCHAR(100),
      PLPT_NM NVARCHAR(200),
      ELECD_NM NVARCHAR(200),
      ELECD_DIV_NM NVARCHAR(200),
      CMIT_NM NVARCHAR(500),
      BLNG_CMIT_NM NTEXT,
      RLCT_DIV_NM NVARCHAR(100),
      GTELT_ERACO NVARCHAR(100),
      NTR_DIV NVARCHAR(10),
      NAAS_TEL_NO NVARCHAR(50),
      NAAS_EMAIL_ADDR NVARCHAR(100),
      NAAS_HP_URL NVARCHAR(200),
      AIDE_NM NVARCHAR(200),
      CHF_SCRT_NM NVARCHAR(200),
      SCRT_NM NVARCHAR(200),
      BRF_HST NTEXT,
      OFFM_RNUM_NO NVARCHAR(50),
      NAAS_PIC NVARCHAR(500),
      created_at DATETIME2 DEFAULT GETDATE()
    )
  `,
  assembly_members_profile: `
    CREATE TABLE assembly_members_profile (
      id INT IDENTITY(1,1) PRIMARY KEY,
      HG_NM NVARCHAR(100),
      HJ_NM NVARCHAR(100),
      ENG_NM NVARCHAR(200),
      BTH_GBN_NM NVARCHAR(50),
      BTH_DATE NVARCHAR(20),
      JOB_RES_NM NVARCHAR(200),
      POLY_NM NVARCHAR(100),
      ORIG_NM NVARCHAR(100),
      ELECT_GBN_NM NVARCHAR(100),
      CMIT_NM NVARCHAR(200),
      CMITS NVARCHAR(200),
      REELE_GBN_NM NVARCHAR(100),
      UNITS NVARCHAR(100),
      SEX_GBN_NM NVARCHAR(20),
      TEL_NO NVARCHAR(50),
      E_MAIL NVARCHAR(100),
      HOMEPAGE NVARCHAR(200),
      STAFF NVARCHAR(500),
      SECRETARY NVARCHAR(200),
      SECRETARY2 NVARCHAR(500),
      MONA_CD NVARCHAR(50),
      MEM_TITLE NTEXT,
      ASSEM_ADDR NVARCHAR(300),
      created_at DATETIME2 DEFAULT GETDATE()
    )
  `,
  assembly_members_combined: `
    CREATE TABLE assembly_members_combined (
      id INT IDENTITY(1,1) PRIMARY KEY,
      member_name NVARCHAR(100),
      mona_cd NVARCHAR(50),
      hg_nm NVARCHAR(100),
      hj_nm NVARCHAR(100),
      naas_nm NVARCHAR(100),
      orig_nm NVARCHAR(100),
      eng_nm NVARCHAR(200),
      bth_gbn_nm NVARCHAR(50),
      bth_date NVARCHAR(20),
      naas_birthday NVARCHAR(20),
      aged NVARCHAR(10),
      naas_age NVARCHAR(10),
      job_res_nm NVARCHAR(200),
      poly_nm NVARCHAR(100),
      naas_party NVARCHAR(100),
      elect_gbn_nm NVARCHAR(100),
      cmit_nm NVARCHAR(200),
      reele_gbn_nm NVARCHAR(100),
      units NVARCHAR(100),
      sex_gbn_nm NVARCHAR(20),
      tel_no NVARCHAR(50),
      naas_tel NVARCHAR(50),
      naas_fax NVARCHAR(50),
      e_mail NVARCHAR(100),
      naas_email NVARCHAR(100),
      homepage NVARCHAR(200),
      naas_homepage NVARCHAR(200),
      staff NVARCHAR(500),
      naas_staff NVARCHAR(500),
      secretary NVARCHAR(200),
      naas_secretary NVARCHAR(200),
      secretary2 NVARCHAR(200),
      naas_assistant NVARCHAR(200),
      naas_research NVARCHAR(200),
      naas_intern NVARCHAR(200),
      assem_addr NVARCHAR(300),
      naas_addr NVARCHAR(300),
      mem_title NTEXT,
      daesu INT,
      naas_education NTEXT,
      naas_career NTEXT,
      naas_pic NVARCHAR(500),
      created_at DATETIME2 DEFAULT GETDATE()
    )
  `
};

async function createTables(pool) {
  console.log('Creating tables...');
  try {
    for (const [tableName, schema] of Object.entries(schemas)) {
      // Check if table exists
      const checkResult = await pool.request()
        .input('tableName', sql.NVarChar, tableName)
        .query(`
          SELECT COUNT(*) as count 
          FROM INFORMATION_SCHEMA.TABLES 
          WHERE TABLE_NAME = @tableName
        `);
      
      if (checkResult.recordset[0].count === 0) {
        await pool.request().query(schema);
        console.log(`Table ${tableName} created.`);
      } else {
        console.log(`Table ${tableName} already exists.`);
      }
    }
  } catch (error) {
    console.error('Error creating tables:', error);
    throw error;
  }
}

function parseDate(dateString) {
  if (!dateString || dateString === 'null') return null;
  try {
    const date = new Date(dateString);
    return isNaN(date.getTime()) ? null : date;
  } catch {
    return null;
  }
}

async function insertBillsData(pool, data, metadata) {
  console.log(`Inserting ${data.length} bills records...`);
  
  for (const item of data) {
    try {
      await pool.request()
        .input('BILL_ID', sql.NVarChar(50), item.BILL_ID)
        .input('BILL_NO', sql.NVarChar(50), item.BILL_NO)
        .input('BILL_NAME', sql.NVarChar(500), item.BILL_NAME)
        .input('COMMITTEE', sql.NVarChar(200), item.COMMITTEE)
        .input('PROPOSE_DT', sql.Date, parseDate(item.PROPOSE_DT))
        .input('PROC_RESULT', sql.NVarChar(100), item.PROC_RESULT)
        .input('AGE', sql.NVarChar(10), item.AGE)
        .input('DETAIL_LINK', sql.NVarChar(1000), item.DETAIL_LINK)
        .input('PROPOSER', sql.NVarChar(500), item.PROPOSER)
        .input('MEMBER_LIST', sql.NVarChar(1000), item.MEMBER_LIST)
        .input('LAW_PROC_DT', sql.Date, parseDate(item.LAW_PROC_DT))
        .input('LAW_PRESENT_DT', sql.Date, parseDate(item.LAW_PRESENT_DT))
        .input('LAW_SUBMIT_DT', sql.Date, parseDate(item.LAW_SUBMIT_DT))
        .input('CMT_PROC_RESULT_CD', sql.NVarChar(100), item.CMT_PROC_RESULT_CD)
        .input('CMT_PROC_DT', sql.Date, parseDate(item.CMT_PROC_DT))
        .input('CMT_PRESENT_DT', sql.Date, parseDate(item.CMT_PRESENT_DT))
        .input('COMMITTEE_DT', sql.Date, parseDate(item.COMMITTEE_DT))
        .input('PROC_DT', sql.Date, parseDate(item.PROC_DT))
        .input('COMMITTEE_ID', sql.NVarChar(50), item.COMMITTEE_ID)
        .input('PUBL_PROPOSER', sql.NVarChar(sql.MAX), item.PUBL_PROPOSER)
        .input('LAW_PROC_RESULT_CD', sql.NVarChar(100), item.LAW_PROC_RESULT_CD)
        .input('RST_PROPOSER', sql.NVarChar(200), item.RST_PROPOSER)
        .query(`
          INSERT INTO assembly_bills (
            BILL_ID, BILL_NO, BILL_NAME, COMMITTEE, PROPOSE_DT, PROC_RESULT, AGE,
            DETAIL_LINK, PROPOSER, MEMBER_LIST, LAW_PROC_DT, LAW_PRESENT_DT, LAW_SUBMIT_DT,
            CMT_PROC_RESULT_CD, CMT_PROC_DT, CMT_PRESENT_DT, COMMITTEE_DT, PROC_DT,
            COMMITTEE_ID, PUBL_PROPOSER, LAW_PROC_RESULT_CD, RST_PROPOSER, age_number
          ) VALUES (
            @BILL_ID, @BILL_NO, @BILL_NAME, @COMMITTEE, @PROPOSE_DT, @PROC_RESULT, @AGE,
            @DETAIL_LINK, @PROPOSER, @MEMBER_LIST, @LAW_PROC_DT, @LAW_PRESENT_DT, @LAW_SUBMIT_DT,
            @CMT_PROC_RESULT_CD, @CMT_PROC_DT, @CMT_PRESENT_DT, @COMMITTEE_DT, @PROC_DT,
            @COMMITTEE_ID, @PUBL_PROPOSER, @LAW_PROC_RESULT_CD, @RST_PROPOSER
          )
        `);
    } catch (error) {
      console.error('Error inserting bill record:', error.message);
    }
  }
}

async function insertMembersHistoryData(pool, data, metadata) {
  console.log(`Inserting ${data.length} members history records...`);
  
  for (const item of data) {
    try {
      await pool.request()
        .input('MONA_CD', sql.NVarChar(50), item.MONA_CD)
        .input('HG_NM', sql.NVarChar(100), item.HG_NM)
        .input('HJ_NM', sql.NVarChar(100), item.HJ_NM)
        .input('ENG_NM', sql.NVarChar(200), item.ENG_NM)
        .input('BTH_GBN_NM', sql.NVarChar(50), item.BTH_GBN_NM)
        .input('BTH_DATE', sql.NVarChar(20), item.BTH_DATE)
        .input('AGED', sql.NVarChar(10), item.AGED)
        .input('JOB_RES_NM', sql.NVarChar(200), item.JOB_RES_NM)
        .input('POLY_NM', sql.NVarChar(100), item.POLY_NM)
        .input('ORIG_NM', sql.NVarChar(100), item.ORIG_NM)
        .input('ELECT_GBN_NM', sql.NVarChar(100), item.ELECT_GBN_NM)
        .input('CMIT_NM', sql.NVarChar(200), item.CMIT_NM)
        .input('REELE_GBN_NM', sql.NVarChar(100), item.REELE_GBN_NM)
        .input('UNITS', sql.NVarChar(100), item.UNITS)
        .input('SEX_GBN_NM', sql.NVarChar(20), item.SEX_GBN_NM)
        .input('TEL_NO', sql.NVarChar(50), item.TEL_NO)
        .input('E_MAIL', sql.NVarChar(100), item.E_MAIL)
        .input('HOMEPAGE', sql.NVarChar(200), item.HOMEPAGE)
        .input('STAFF', sql.NVarChar(500), item.STAFF)
        .input('SECRETARY', sql.NVarChar(200), item.SECRETARY)
        .input('SECRETARY2', sql.NVarChar(200), item.SECRETARY2)
        .input('ASSEM_ADDR', sql.NVarChar(300), item.ASSEM_ADDR)
        .input('MEM_TITLE', sql.NText, item.MEM_TITLE)
        .input('DAESU', sql.Int, metadata.daesu || null)
        .query(`
          INSERT INTO assembly_members_history (
            MONA_CD, HG_NM, HJ_NM, ENG_NM, BTH_GBN_NM, BTH_DATE, AGED, JOB_RES_NM, POLY_NM, ORIG_NM,
            ELECT_GBN_NM, CMIT_NM, REELE_GBN_NM, UNITS, SEX_GBN_NM, TEL_NO,
            E_MAIL, HOMEPAGE, STAFF, SECRETARY, SECRETARY2, ASSEM_ADDR, MEM_TITLE, DAESU
          ) VALUES (
            @MONA_CD, @HG_NM, @HJ_NM, @ENG_NM, @BTH_GBN_NM, @BTH_DATE, @AGED, @JOB_RES_NM, @POLY_NM, @ORIG_NM,
            @ELECT_GBN_NM, @CMIT_NM, @REELE_GBN_NM, @UNITS, @SEX_GBN_NM, @TEL_NO,
            @E_MAIL, @HOMEPAGE, @STAFF, @SECRETARY, @SECRETARY2, @ASSEM_ADDR, @MEM_TITLE, @DAESU
          )
        `);
    } catch (error) {
      console.error('Error inserting member history record:', error.message);
    }
  }
}

async function insertMembersHistoryDaesuData(pool, data) {
  console.log(`Inserting ${data.length} members history daesu records...`);
  
  for (const item of data) {
    try {
      await pool.request()
        .input('DAESU', sql.NVarChar(10), item.DAESU)
        .input('DAE', sql.NText, item.DAE)
        .input('DAE_NM', sql.NVarChar(100), item.DAE_NM)
        .input('NAME', sql.NVarChar(100), item.NAME)
        .input('NAME_HAN', sql.NVarChar(100), item.NAME_HAN)
        .input('JA', sql.NVarChar(100), item.JA)
        .input('HO', sql.NText, item.HO)
        .input('BIRTH', sql.NVarChar(50), item.BIRTH)
        .input('BON', sql.NVarChar(100), item.BON)
        .input('POSI', sql.NVarChar(200), item.POSI)
        .input('HAK', sql.NText, item.HAK)
        .input('HOBBY', sql.NVarChar(500), item.HOBBY)
        .input('BOOK', sql.NText, item.BOOK)
        .input('SANG', sql.NText, item.SANG)
        .input('DEAD', sql.NVarChar(50), item.DEAD)
        .input('URL', sql.NVarChar(500), item.URL)
        .query(`
          INSERT INTO assembly_members_history_daesu (
            DAESU, DAE, DAE_NM, NAME, NAME_HAN, JA, HO, BIRTH, BON, POSI,
            HAK, HOBBY, BOOK, SANG, DEAD, URL
          ) VALUES (
            @DAESU, @DAE, @DAE_NM, @NAME, @NAME_HAN, @JA, @HO, @BIRTH, @BON, @POSI,
            @HAK, @HOBBY, @BOOK, @SANG, @DEAD, @URL
          )
        `);
    } catch (error) {
      console.error('Error inserting member history daesu record:', error.message);
    }
  }
}

async function insertMembersIntegratedData(pool, data) {
  console.log(`Inserting ${data.length} integrated members records...`);
  
  for (const item of data) {
    try {
      await pool.request()
        .input('NAAS_CD', sql.NVarChar(50), item.NAAS_CD)
        .input('NAAS_NM', sql.NVarChar(100), item.NAAS_NM)
        .input('NAAS_CH_NM', sql.NVarChar(100), item.NAAS_CH_NM)
        .input('NAAS_EN_NM', sql.NVarChar(200), item.NAAS_EN_NM)
        .input('BIRDY_DIV_CD', sql.NVarChar(10), item.BIRDY_DIV_CD)
        .input('BIRDY_DT', sql.NVarChar(20), item.BIRDY_DT)
        .input('DTY_NM', sql.NVarChar(100), item.DTY_NM)
        .input('PLPT_NM', sql.NVarChar(200), item.PLPT_NM)
        .input('ELECD_NM', sql.NVarChar(200), item.ELECD_NM)
        .input('ELECD_DIV_NM', sql.NVarChar(200), item.ELECD_DIV_NM)
        .input('CMIT_NM', sql.NVarChar(500), item.CMIT_NM)
        .input('BLNG_CMIT_NM', sql.NText, item.BLNG_CMIT_NM)
        .input('RLCT_DIV_NM', sql.NVarChar(100), item.RLCT_DIV_NM)
        .input('GTELT_ERACO', sql.NVarChar(100), item.GTELT_ERACO)
        .input('NTR_DIV', sql.NVarChar(10), item.NTR_DIV)
        .input('NAAS_TEL_NO', sql.NVarChar(50), item.NAAS_TEL_NO)
        .input('NAAS_EMAIL_ADDR', sql.NVarChar(100), item.NAAS_EMAIL_ADDR)
        .input('NAAS_HP_URL', sql.NVarChar(200), item.NAAS_HP_URL)
        .input('AIDE_NM', sql.NVarChar(200), item.AIDE_NM)
        .input('CHF_SCRT_NM', sql.NVarChar(200), item.CHF_SCRT_NM)
        .input('SCRT_NM', sql.NVarChar(200), item.SCRT_NM)
        .input('BRF_HST', sql.NText, item.BRF_HST)
        .input('OFFM_RNUM_NO', sql.NVarChar(50), item.OFFM_RNUM_NO)
        .input('NAAS_PIC', sql.NVarChar(500), item.NAAS_PIC)
        .query(`
          INSERT INTO assembly_members_integrated (
            NAAS_CD, NAAS_NM, NAAS_CH_NM, NAAS_EN_NM, BIRDY_DIV_CD, BIRDY_DT, DTY_NM,
            PLPT_NM, ELECD_NM, ELECD_DIV_NM, CMIT_NM, BLNG_CMIT_NM, RLCT_DIV_NM,
            GTELT_ERACO, NTR_DIV, NAAS_TEL_NO, NAAS_EMAIL_ADDR, NAAS_HP_URL,
            AIDE_NM, CHF_SCRT_NM, SCRT_NM, BRF_HST, OFFM_RNUM_NO, NAAS_PIC
          ) VALUES (
            @NAAS_CD, @NAAS_NM, @NAAS_CH_NM, @NAAS_EN_NM, @BIRDY_DIV_CD, @BIRDY_DT, @DTY_NM,
            @PLPT_NM, @ELECD_NM, @ELECD_DIV_NM, @CMIT_NM, @BLNG_CMIT_NM, @RLCT_DIV_NM,
            @GTELT_ERACO, @NTR_DIV, @NAAS_TEL_NO, @NAAS_EMAIL_ADDR, @NAAS_HP_URL,
            @AIDE_NM, @CHF_SCRT_NM, @SCRT_NM, @BRF_HST, @OFFM_RNUM_NO, @NAAS_PIC
          )
        `);
    } catch (error) {
      console.error('Error inserting integrated member record:', error.message);
    }
  }
}

async function insertMembersProfileData(pool, data) {
  console.log(`Inserting ${data.length} profile records...`);
  
  for (const item of data) {
    try {
      await pool.request()
        .input('HG_NM', sql.NVarChar(100), item.HG_NM)
        .input('HJ_NM', sql.NVarChar(100), item.HJ_NM)
        .input('ENG_NM', sql.NVarChar(200), item.ENG_NM)
        .input('BTH_GBN_NM', sql.NVarChar(50), item.BTH_GBN_NM)
        .input('BTH_DATE', sql.NVarChar(20), item.BTH_DATE)
        .input('JOB_RES_NM', sql.NVarChar(200), item.JOB_RES_NM)
        .input('POLY_NM', sql.NVarChar(100), item.POLY_NM)
        .input('ORIG_NM', sql.NVarChar(100), item.ORIG_NM)
        .input('ELECT_GBN_NM', sql.NVarChar(100), item.ELECT_GBN_NM)
        .input('CMIT_NM', sql.NVarChar(200), item.CMIT_NM)
        .input('CMITS', sql.NVarChar(200), item.CMITS)
        .input('REELE_GBN_NM', sql.NVarChar(100), item.REELE_GBN_NM)
        .input('UNITS', sql.NVarChar(100), item.UNITS)
        .input('SEX_GBN_NM', sql.NVarChar(20), item.SEX_GBN_NM)
        .input('TEL_NO', sql.NVarChar(50), item.TEL_NO)
        .input('E_MAIL', sql.NVarChar(100), item.E_MAIL)
        .input('HOMEPAGE', sql.NVarChar(200), item.HOMEPAGE)
        .input('STAFF', sql.NVarChar(500), item.STAFF)
        .input('SECRETARY', sql.NVarChar(200), item.SECRETARY)
        .input('SECRETARY2', sql.NVarChar(500), item.SECRETARY2)
        .input('MONA_CD', sql.NVarChar(50), item.MONA_CD)
        .input('MEM_TITLE', sql.NText, item.MEM_TITLE)
        .input('ASSEM_ADDR', sql.NVarChar(300), item.ASSEM_ADDR)
        .query(`
          INSERT INTO assembly_members_profile (
            HG_NM, HJ_NM, ENG_NM, BTH_GBN_NM, BTH_DATE, JOB_RES_NM, POLY_NM, ORIG_NM,
            ELECT_GBN_NM, CMIT_NM, CMITS, REELE_GBN_NM, UNITS, SEX_GBN_NM, TEL_NO,
            E_MAIL, HOMEPAGE, STAFF, SECRETARY, SECRETARY2, MONA_CD, MEM_TITLE, ASSEM_ADDR
          ) VALUES (
            @HG_NM, @HJ_NM, @ENG_NM, @BTH_GBN_NM, @BTH_DATE, @JOB_RES_NM, @POLY_NM, @ORIG_NM,
            @ELECT_GBN_NM, @CMIT_NM, @CMITS, @REELE_GBN_NM, @UNITS, @SEX_GBN_NM, @TEL_NO,
            @E_MAIL, @HOMEPAGE, @STAFF, @SECRETARY, @SECRETARY2, @MONA_CD, @MEM_TITLE, @ASSEM_ADDR
          )
        `);
    } catch (error) {
      console.error('Error inserting profile record:', error.message);
    }
  }
}

async function createCombinedTable(pool) {
  console.log('Creating combined members table...');
  
  try {
    await pool.request().query(`
      DELETE FROM assembly_members_combined;
      
      INSERT INTO assembly_members_combined (
        member_name, mona_cd, hg_nm, hj_nm, naas_nm, orig_nm, eng_nm, bth_gbn_nm, bth_date, naas_birthday,
        aged, naas_age, job_res_nm, poly_nm, naas_party, elect_gbn_nm, cmit_nm, reele_gbn_nm,
        units, sex_gbn_nm, tel_no, naas_tel, naas_fax, e_mail, naas_email, homepage,
        naas_homepage, staff, naas_staff, secretary, naas_secretary, secretary2,
        naas_assistant, naas_research, naas_intern, assem_addr, naas_addr, mem_title,
        daesu, naas_education, naas_career, naas_pic
      )
      SELECT 
        COALESCE(h.HG_NM, p.HG_NM, i.NAAS_NM) as member_name,
        COALESCE(h.MONA_CD, p.MONA_CD, i.NAAS_CD) as mona_cd,
        COALESCE(h.HG_NM, p.HG_NM) as hg_nm, COALESCE(h.HJ_NM, p.HJ_NM) as hj_nm, i.NAAS_NM as naas_nm, 
        COALESCE(h.ORIG_NM, p.ORIG_NM) as orig_nm, COALESCE(h.ENG_NM, p.ENG_NM) as eng_nm,
        COALESCE(h.BTH_GBN_NM, p.BTH_GBN_NM) as bth_gbn_nm, COALESCE(h.BTH_DATE, p.BTH_DATE) as bth_date, 
        NULL as naas_birthday, h.AGED, NULL as naas_age, COALESCE(h.JOB_RES_NM, p.JOB_RES_NM) as job_res_nm,
        COALESCE(h.POLY_NM, p.POLY_NM) as poly_nm, NULL as naas_party, COALESCE(h.ELECT_GBN_NM, p.ELECT_GBN_NM) as elect_gbn_nm,
        COALESCE(h.CMIT_NM, p.CMIT_NM) as cmit_nm, COALESCE(h.REELE_GBN_NM, p.REELE_GBN_NM) as reele_gbn_nm,
        COALESCE(h.UNITS, p.UNITS) as units, COALESCE(h.SEX_GBN_NM, p.SEX_GBN_NM) as sex_gbn_nm,
        COALESCE(h.TEL_NO, p.TEL_NO) as tel_no, NULL as naas_tel, NULL as naas_fax,
        COALESCE(h.E_MAIL, p.E_MAIL) as e_mail, NULL as naas_email, COALESCE(h.HOMEPAGE, p.HOMEPAGE) as homepage,
        NULL as naas_homepage, COALESCE(h.STAFF, p.STAFF) as staff, NULL as naas_staff,
        COALESCE(h.SECRETARY, p.SECRETARY) as secretary, NULL as naas_secretary,
        COALESCE(h.SECRETARY2, p.SECRETARY2) as secretary2, NULL as naas_assistant,
        NULL as naas_research, NULL as naas_intern, COALESCE(h.ASSEM_ADDR, p.ASSEM_ADDR) as assem_addr,
        NULL as naas_addr, COALESCE(h.MEM_TITLE, p.MEM_TITLE) as mem_title, h.DAESU,
        NULL as naas_education, NULL as naas_career, i.NAAS_PIC as naas_pic
      FROM assembly_members_history h
      FULL OUTER JOIN assembly_members_profile p ON h.HG_NM = p.HG_NM OR h.MONA_CD = p.MONA_CD
      FULL OUTER JOIN assembly_members_integrated i ON COALESCE(h.HG_NM, p.HG_NM) = i.NAAS_NM OR COALESCE(h.MONA_CD, p.MONA_CD) = i.NAAS_CD
    `);
    
    console.log('Combined members table created successfully.');
  } catch (error) {
    console.error('Error creating combined table:', error);
  }
}

async function loadJSONFile(filePath) {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return JSON.parse(content);
  } catch (error) {
    console.error(`Error reading file ${filePath}:`, error.message);
    return null;
  }
}

async function main() {
  let pool;
  
  try {
    console.log('Connecting to Azure SQL Database...');
    pool = await sql.connect(config);
    console.log('Connected successfully!');
    
    await createTables(pool);
    
    // Get all assembly JSON files
    const files = await fs.readdir(__dirname);
    const assemblyFiles = files.filter(file => 
      file.startsWith('assembly_') && file.endsWith('.json')
    );
    
    console.log(`Found ${assemblyFiles.length} assembly JSON files to process.`);
    
    for (const file of assemblyFiles) {
      console.log(`\nProcessing file: ${file}`);
      const filePath = path.join(__dirname, file);
      const jsonData = await loadJSONFile(filePath);
      
      if (!jsonData || !jsonData.data || jsonData.data.length === 0) {
        console.log(`Skipping ${file} - no data found or empty data array.`);
        continue;
      }
      
      const metadata = {
        daesu: jsonData.daesu,
        age: jsonData.age
      };
      
      // Determine table based on filename
      if (file.includes('bills')) {
        // await insertBillsData(pool, jsonData.data, metadata);
      } else if (file.includes('members_history_daesu')) {
        await insertMembersHistoryDaesuData(pool, jsonData.data);
      } else if (file.includes('history')) {
        await insertMembersHistoryData(pool, jsonData.data, metadata);
      } else if (file.includes('integrated')) {
        await insertMembersIntegratedData(pool, jsonData.data);
      } else if (file.includes('profile')) {
        await insertMembersProfileData(pool, jsonData.data);
      }
      
      console.log(`Completed processing ${file}`);
    }
    
    // Create combined table
    await createCombinedTable(pool);
    
    console.log('\nAll data loaded successfully!');
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    if (pool) {
      await pool.close();
      console.log('Database connection closed.');
    }
  }
}

if (require.main === module) {
  main();
}

module.exports = { main };
