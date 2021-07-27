# Import libraries
import pandas as pd
import psycopg2


# information used to create a database connection
sqluser = 'postgres'
dbname = 'mimic4'
hostname = 'localhost'
port_number = 5434
schema_name = 'mimiciv'

# Connect to postgres with a copy of the MIMIC-III database
con = psycopg2.connect(dbname=dbname, user=sqluser, host=hostname, port=port_number, password='mysecretpassword')

# the below statement is prepended to queries to ensure they select from the right schema
query_schema = 'set search_path to ' + schema_name + ';'


def getStaticFeatures():
    query = query_schema + \
    """
    WITH ht AS
    (
      SELECT 
        c.subject_id, c.stay_id, c.charttime,
        -- Ensure that all heights are in centimeters, and fix data as needed
        CASE
            -- rule for neonates
            WHEN pt.anchor_age = 0
             AND (c.valuenum * 2.54) < 80
              THEN c.valuenum * 2.54
            -- rule for adults
            WHEN pt.anchor_age > 0
             AND (c.valuenum * 2.54) > 120
             AND (c.valuenum * 2.54) < 230
              THEN c.valuenum * 2.54
            -- set bad data to NULL
            ELSE NULL
        END AS height
        , ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime DESC) AS rn
      FROM mimiciv.chartevents c
      INNER JOIN mimiciv.patients pt
        ON c.subject_id = pt.subject_id
      WHERE c.valuenum IS NOT NULL
      AND c.valuenum != 0
      AND c.itemid IN
      (
          226707 -- Height (measured in inches)
        -- note we intentionally ignore the below ITEMID in metavision
        -- these are duplicate data in a different unit
        -- , 226730 -- Height (cm)
      )
    )
    , wt AS
    (
        SELECT
            c.stay_id
          , c.charttime
          -- TODO: eliminate obvious outliers if there is a reasonable weight
          , c.valuenum as weight
          , ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime DESC) AS rn
        FROM mimiciv.chartevents c
        WHERE c.valuenum IS NOT NULL
          AND c.itemid = 226512 -- Admit Wt
          AND c.stay_id IS NOT NULL
          AND c.valuenum > 0
    )
    SELECT
    ie.subject_id, ie.hadm_id, ie.stay_id
    , CASE WHEN pat.gender = 'M' THEN '1' ELSE '0' END AS is_male
    , FLOOR(DATE_PART('day', adm.admittime - make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age as age
    , CASE WHEN adm.ethnicity LIKE '%WHITE%' THEN '1' ELSE '0' END AS race_white
    , CASE WHEN adm.ethnicity LIKE '%BLACK%' THEN '1' ELSE '0' END AS race_black
    , CASE WHEN adm.ethnicity LIKE '%HISPANIC%' THEN '1' ELSE '0' END AS race_hispanic
    , CASE WHEN adm.ethnicity LIKE '%ASIAN%' THEN '1' ELSE '0' END AS race_asian
    , CASE WHEN adm.ethnicity LIKE '%OTHER%' THEN '1' ELSE '0' END AS race_other
    , CASE WHEN adm.admission_type LIKE '%EMER%' THEN '1' ELSE '0' END AS emergency_admission
    , CASE
        WHEN ht.height IS NOT null AND wt.weight IS NOT null
            THEN (wt.weight / (ht.height/100*ht.height/100))
        ELSE null
    END AS bmi
    , ht.height as height
    , wt.weight as weight
    , (
        SELECT
        CASE WHEN COUNT(*) = 0 THEN 0 ELSE 1 END
        FROM mimiciv.transfers car_trs
        WHERE car_trs.hadm_id = adm.hadm_id
        AND lower(car_trs.careunit) LIKE '%card%'
        AND lower(car_trs.careunit) LIKE '%surg%'
    ) AS service_any_card_surg
    , (
        SELECT
        CASE WHEN COUNT(*) = 0 THEN 0 ELSE 1 END
        FROM mimiciv.transfers car_trs
        WHERE car_trs.hadm_id = adm.hadm_id
        AND lower(car_trs.careunit) NOT LIKE '%card%'
        AND lower(car_trs.careunit) LIKE '%surg%'
    ) AS service_any_noncard_surg
    , (
        SELECT
        CASE WHEN COUNT(*) = 0 THEN 0 ELSE 1 END
        FROM mimiciv.transfers car_trs
        WHERE car_trs.hadm_id = adm.hadm_id
        AND lower(car_trs.careunit) LIKE '%trauma%'
    ) AS service_trauma
    -- , adm.hospital_expire_flag
    FROM mimiciv.icustays ie
    INNER JOIN mimiciv.admissions adm
    ON ie.hadm_id = adm.hadm_id
    INNER JOIN mimiciv.patients pat
    ON ie.subject_id = pat.subject_id
    LEFT JOIN ht
    ON ie.stay_id = ht.stay_id AND ht.rn = 1
    LEFT JOIN wt
    ON ie.stay_id = wt.stay_id AND wt.rn = 1
    """

    static = pd.read_sql_query(query, con)
    return static


def getLabFeatures(mode='first', duration=24, pre_admission_lookback=8):
    query = query_schema + \
    """
    WITH labs_preceeding AS
    (
        SELECT icu.stay_id, l.valuenum, l.charttime
        , CASE
                WHEN itemid = 51006 THEN 'BUN'
                WHEN itemid = 50806 THEN 'CHLORIDE'
                WHEN itemid = 50902 THEN 'CHLORIDE'
                WHEN itemid = 50912 THEN 'CREATININE'
                WHEN itemid = 50811 THEN 'HEMOGLOBIN'
                WHEN itemid = 51222 THEN 'HEMOGLOBIN'
                WHEN itemid = 51265 THEN 'PLATELET'
                WHEN itemid = 50822 THEN 'POTASSIUM'
                WHEN itemid = 50971 THEN 'POTASSIUM'
                WHEN itemid = 50824 THEN 'SODIUM'
                WHEN itemid = 50983 THEN 'SODIUM'
                WHEN itemid = 50803 THEN 'BICARBONATE'
                WHEN itemid = 50882 THEN 'BICARBONATE'
                WHEN itemid = 50804 THEN 'TOTALCO2'
                WHEN itemid = 50821 THEN 'PO2'
                WHEN itemid = 52042 THEN 'PO2'
                WHEN itemid = 50832 THEN 'PO2'
                WHEN itemid = 50818 THEN 'PCO2'
                WHEN itemid = 52040 THEN 'PCO2'
                WHEN itemid = 50830 THEN 'PCO2'
                WHEN itemid = 50820 THEN 'PH'
                WHEN itemid = 52041 THEN 'PH'
                WHEN itemid = 50831 THEN 'PH'
                WHEN itemid = 51300 THEN 'WBC'
                WHEN itemid = 51301 THEN 'WBC'
                WHEN itemid = 50802 THEN 'BASEEXCESS'
                WHEN itemid = 52038 THEN 'BASEEXCESS'
                WHEN itemid = 50805 THEN 'CARBOXYHEMOGLOBIN'
                WHEN itemid = 50814 THEN 'METHEMOGLOBIN'
                WHEN itemid = 50868 THEN 'ANIONGAP'
                WHEN itemid = 52500 THEN 'ANIONGAP'
                WHEN itemid = 50862 THEN 'ALBUMIN'
                WHEN itemid = 51144 THEN 'BANDS'
                WHEN itemid = 50885 THEN 'BILRUBIN'
                WHEN itemid = 51478 THEN 'GLUCOSE'
                WHEN itemid = 50931 THEN 'GLUCOSE'
                WHEN itemid = 51221 THEN 'HEMATOCRIT'
                WHEN itemid = 50813 THEN 'LACTATE'
                WHEN itemid = 51275 THEN 'PTT'
                WHEN itemid = 51237 THEN 'INR'
              ELSE null
            END AS LABEL
        FROM mimiciv.icustays icu
        INNER JOIN mimiciv.admissions adm
        ON icu.hadm_id = adm.hadm_id
        INNER JOIN mimiciv.patients pat
        ON icu.subject_id = pat.subject_id
        INNER JOIN mimiciv.labevents l
        ON l.hadm_id = icu.hadm_id
        AND l.charttime >= icu.intime - interval '""" + str(pre_admission_lookback) + """ hour'
        AND l.charttime <= icu.intime + interval '""" + str(duration) + """ hour'
        WHERE l.itemid IN
        (
         51300,51301 -- wbc
        , 50811,51222 -- hgb
        , 51265 -- platelet
        , 50824, 50983 -- sodium
        , 50822, 50971 -- potassium
        , 50804 -- Total CO2 or ...
        , 50803, 50882  -- bicarbonate
        , 50806, 50902 -- chloride
        , 51006 -- bun
        , 50912 -- creatinine
        , 50821, 52042, 50832 -- po2
        , 50818, 52040, 50830 -- pco2
        , 50820, 52041, 50831 -- ph
        , 50802, 52038 -- Base Excess
        , 50805 -- carboxyhemoglobin
        , 50814 -- methemoglobin
        , 50868, 52500 -- aniongap
        , 50862 -- albumin
        , 51144 -- bands
        , 50885 -- bilrubin
        , 51478, 50931 -- glucose
        , 51221 -- hematocrit
        , 50813 -- lactate
        , 51275 -- ptt
        , 51237 -- inr
        )
        AND valuenum IS NOT null
    )
    , labs_rn AS
    (
      SELECT
        stay_id, valuenum, label
        , ROW_NUMBER() OVER (PARTITION BY stay_id, label ORDER BY charttime""" + (' DESC' if (mode == 'last') else ' ASC') + """) AS rn
      FROM labs_preceeding
    )
    , labs_grp AS
    (
      SELECT
        stay_id
        , COALESCE(MAX(CASE WHEN label = 'BUN' THEN valuenum ELSE null END)) AS BUN
        , COALESCE(MAX(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END)) AS CHLORIDE
        , COALESCE(MAX(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END)) AS CREATININE
        , COALESCE(MAX(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END)) AS HEMOGLOBIN
        , COALESCE(MAX(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END)) AS PLATELET
        , COALESCE(MAX(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END)) AS POTASSIUM
        , COALESCE(MAX(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END)) AS SODIUM
        , COALESCE(MAX(CASE WHEN label = 'TOTALCO2' THEN valuenum ELSE null END)) AS TOTALCO2
        , COALESCE(MAX(CASE WHEN label = 'WBC' THEN valuenum ELSE null END)) AS WBC
        , COALESCE(MAX(CASE WHEN label = 'PO2' THEN valuenum ELSE null END)) AS PO2
        , COALESCE(MAX(CASE WHEN label = 'PCO2' THEN valuenum ELSE null END)) AS PCO2
        , COALESCE(MAX(CASE WHEN label = 'PH' THEN valuenum ELSE null END)) AS PH
        , COALESCE(MAX(CASE WHEN label = 'BASEEXCESS' THEN valuenum ELSE null END)) AS BASEEXCESS
        , COALESCE(MAX(CASE WHEN label = 'CARBOXYHEMOGLOBIN' THEN valuenum ELSE null END)) AS CARBOXYHEMOGLOBIN
        , COALESCE(MAX(CASE WHEN label = 'METHEMOGLOBIN' THEN valuenum ELSE null END)) AS METHEMOGLOBIN
        , COALESCE(MAX(CASE WHEN label = 'ANIONGAP' THEN valuenum ELSE null END)) AS ANIONGAP
        , COALESCE(MAX(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END)) AS ALBUMIN
        , COALESCE(MAX(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END)) AS BANDS
        , COALESCE(MAX(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END)) AS BICARBONATE
        , COALESCE(MAX(CASE WHEN label = 'BILRUBIN' THEN valuenum ELSE null END)) AS BILRUBIN
        , COALESCE(MAX(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END)) AS GLUCOSE
        , COALESCE(MAX(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END)) AS HEMATOCRIT
        , COALESCE(MAX(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END)) AS LACTATE
        , COALESCE(MAX(CASE WHEN label = 'PTT' THEN valuenum ELSE null END)) AS PTT
        , COALESCE(MAX(CASE WHEN label = 'INR' THEN valuenum ELSE null END)) AS INR
      FROM labs_rn
      WHERE rn = 1
      GROUP BY stay_id
    )
    SELECT icu.stay_id
      , lg.bun AS bun_""" + mode + """
      , lg.chloride AS chloride_""" + mode + """
      , lg.creatinine AS creatinine_""" + mode + """
      , lg.HEMOGLOBIN AS hgb_""" + mode + """
      , lg.platelet AS platelet_""" + mode + """
      , lg.potassium AS potassium_""" + mode + """
      , lg.sodium AS sodium_""" + mode + """
      , lg.TOTALCO2 AS tco2_""" + mode + """
      , lg.wbc AS wbc_""" + mode + """
      , lg.po2 AS bg_po2_""" + mode + """
      , lg.pco2 AS bg_pco2_""" + mode + """
      , lg.ph AS bg_ph_""" + mode + """
      , lg.BASEEXCESS AS bg_baseexcess_""" + mode + """
      , lg.CARBOXYHEMOGLOBIN AS bg_carboxyhemoglobin_""" + mode + """
      , lg.METHEMOGLOBIN AS bg_methemomoglobin_""" + mode + """
      , lg.ANIONGAP AS aniongap_""" + mode + """
      , lg.ALBUMIN AS albumin_""" + mode + """
      , lg.BANDS AS bands_""" + mode + """
      , lg.BICARBONATE AS bicarbonate_""" + mode + """
      , lg.BILRUBIN AS bilrubin_""" + mode + """
      , lg.GLUCOSE AS glucose_""" + mode + """
      , lg.HEMATOCRIT AS hematocrit_""" + mode + """
      , lg.LACTATE AS lactate_""" + mode + """
      , lg.PTT AS ptt_""" + mode + """
      , lg.INR AS inr_""" + mode + """
    FROM mimiciv.icustays icu
    LEFT JOIN labs_grp lg
    ON icu.stay_id = lg.stay_id
    """

    lab = pd.read_sql_query(query, con)

    return lab


def getVitalsFeatures(mode='first', duration=24):
    query = query_schema + \
    """
    WITH vitals_stg_1 AS
    (
        SELECT icu.stay_id, cev.charttime
        , CASE
                WHEN itemid = 223761 THEN (cev.valuenum-32)/1.8
            ELSE cev.valuenum
        END AS valuenum
        , CASE
                WHEN itemid = 220045 THEN 'HEARTRATE'
                WHEN itemid = 220050 THEN 'SYSBP'
                WHEN itemid = 220179 THEN 'SYSBP'
                WHEN itemid = 220051 THEN 'DIASBP'
                WHEN itemid = 220180 THEN 'DIASBP'
                WHEN itemid = 220052 THEN 'MEANBP'
                WHEN itemid = 220181 THEN 'MEANBP'
                WHEN itemid = 225312 THEN 'MEANBP'
                WHEN itemid = 220210 THEN 'RESPRATE'
                WHEN itemid = 224688 THEN 'RESPRATE'
                WHEN itemid = 224689 THEN 'RESPRATE'
                WHEN itemid = 224690 THEN 'RESPRATE'
                WHEN itemid = 223761 THEN 'TEMPC'
                WHEN itemid = 223762 THEN 'TEMPC'
                WHEN itemid = 220277 THEN 'SPO2'
                WHEN itemid = 220739 THEN 'GCSEYE'
                WHEN itemid = 223900 THEN 'GCSVERBAL'
                WHEN itemid = 223901 THEN 'GCSMOTOR'
              ELSE null
            END AS label
        FROM mimiciv.icustays icu
        INNER JOIN mimiciv.chartevents cev
        ON cev.stay_id = icu.stay_id
        AND cev.charttime >= icu.intime
        AND cev.charttime <= icu.intime + interval '""" + str(duration) + """ hour'
        WHERE cev.itemid IN
        (
         220045 -- heartrate
        , 220050, 220179 -- sysbp
        , 220051, 220180 -- diasbp
        , 220052, 220181, 225312 -- meanbp
        , 220210, 224688, 224689, 224690 -- resprate
        , 223761, 223762 -- tempc
        , 220277 -- SpO2
        , 220739 -- gcseye
        , 223900 -- gcsverbal
        , 223901 -- gscmotor
        )
        AND valuenum IS NOT null
    )
    , vitals_stg_2 AS
    (
      SELECT
        stay_id, valuenum, label
        , ROW_NUMBER() OVER (PARTITION BY stay_id, label ORDER BY charttime""" + (' DESC' if (mode == 'last') else ' ASC') + """) AS rn
      FROM vitals_stg_1
    )
    , vitals_stg_3 AS
    (
      SELECT
        stay_id
        , COALESCE(MAX(CASE WHEN label = 'HEARTRATE' THEN valuenum ELSE null END)) AS heartrate_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'SYSBP' THEN valuenum ELSE null END)) AS sysbp_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'DIASBP' THEN valuenum ELSE null END)) AS diabp_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'MEANBP' THEN valuenum ELSE null END)) AS meanbp_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'RESPRATE' THEN valuenum ELSE null END)) AS resprate_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'TEMPC' THEN valuenum ELSE null END)) AS tempc_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'SPO2' THEN valuenum ELSE null END)) AS spo2_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'GCSEYE' THEN valuenum ELSE null END)) AS gcseye_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'GCSVERBAL' THEN valuenum ELSE null END)) AS gcsverbal_""" + mode + """
        , COALESCE(MAX(CASE WHEN label = 'GCSMOTOR' THEN valuenum ELSE null END)) AS gcsmotor_""" + mode + """
      FROM vitals_stg_2
      WHERE rn = 1
      GROUP BY stay_id
    )
    SELECT * FROM vitals_stg_3
    """

    first_vitals = pd.read_sql_query(query, con)

    return first_vitals


def getMinMaxVitalsFeatures(mode='min', duration=24):
    query = query_schema + \
    """
    WITH vitals_stg_1 AS
    (
        SELECT icu.stay_id, cev.charttime
        , CASE
                WHEN itemid = 223761 THEN (cev.valuenum-32)/1.8
            ELSE cev.valuenum
        END AS valuenum
        , CASE
                WHEN itemid = 220045 THEN 'HEARTRATE'
                WHEN itemid = 220050 THEN 'SYSBP'
                WHEN itemid = 220179 THEN 'SYSBP'
                WHEN itemid = 220051 THEN 'DIASBP'
                WHEN itemid = 220180 THEN 'DIASBP'
                WHEN itemid = 220052 THEN 'MEANBP'
                WHEN itemid = 220181 THEN 'MEANBP'
                WHEN itemid = 225312 THEN 'MEANBP'
                WHEN itemid = 220210 THEN 'RESPRATE'
                WHEN itemid = 224688 THEN 'RESPRATE'
                WHEN itemid = 224689 THEN 'RESPRATE'
                WHEN itemid = 224690 THEN 'RESPRATE'
                WHEN itemid = 223761 THEN 'TEMPC'
                WHEN itemid = 223762 THEN 'TEMPC'
                WHEN itemid = 220277 THEN 'SPO2'
                WHEN itemid = 220739 THEN 'GCSEYE'
                WHEN itemid = 223900 THEN 'GCSVERBAL'
                WHEN itemid = 223901 THEN 'GCSMOTOR'
              ELSE null
            END AS label
        FROM mimiciv.icustays icu
        INNER JOIN mimiciv.chartevents cev
        ON cev.stay_id = icu.stay_id
        AND cev.charttime >= icu.intime
        AND cev.charttime <= icu.intime + interval '""" + str(duration) + """ hour'
        WHERE cev.itemid IN
        (
         220045 -- heartrate
        , 220050, 220179 -- sysbp
        , 220051, 220180 -- diasbp
        , 220052, 220181, 225312 -- meanbp
        , 220210, 224688, 224689, 224690 -- resprate
        , 223761, 223762 -- tempc
        , 220277 -- SpO2
        , 220739 -- gcseye
        , 223900 -- gcsverbal
        , 223901 -- gscmotor
        )
        AND valuenum IS NOT null
    )
    , vitals_stg_2 AS
    (
      SELECT
        stay_id, valuenum, label
        , ROW_NUMBER() OVER (PARTITION BY stay_id, label ORDER BY charttime) AS rn
      FROM vitals_stg_1
    )
    , vitals_stg_3 AS
    (
      SELECT
        stay_id
        , rn
        , COALESCE(MAX(CASE WHEN label = 'HEARTRATE' THEN valuenum ELSE null END)) AS heartrate
        , COALESCE(MAX(CASE WHEN label = 'SYSBP' THEN valuenum ELSE null END)) AS sysbp
        , COALESCE(MAX(CASE WHEN label = 'DIASBP' THEN valuenum ELSE null END)) AS diabp
        , COALESCE(MAX(CASE WHEN label = 'MEANBP' THEN valuenum ELSE null END)) AS meanbp
        , COALESCE(MAX(CASE WHEN label = 'RESPRATE' THEN valuenum ELSE null END)) AS resprate
        , COALESCE(MAX(CASE WHEN label = 'TEMPC' THEN valuenum ELSE null END)) AS tempc
        , COALESCE(MAX(CASE WHEN label = 'SPO2' THEN valuenum ELSE null END)) AS spo2
        , COALESCE(MAX(CASE WHEN label = 'GCSEYE' THEN valuenum ELSE null END)) AS gcseye
        , COALESCE(MAX(CASE WHEN label = 'GCSVERBAL' THEN valuenum ELSE null END)) AS gcsverbal
        , COALESCE(MAX(CASE WHEN label = 'GCSMOTOR' THEN valuenum ELSE null END)) AS gcsmotor
      FROM vitals_stg_2
      GROUP BY stay_id, rn
    )
    , vitals_stg_4 AS
    (
        SELECT
        stay_id,
        """ + mode.upper() + """(heartrate) AS heartrate_""" + mode + """
        , """ + mode.upper() + """(sysbp) AS sysbp_""" + mode + """
        , """ + mode.upper() + """(diabp) AS diabp_""" + mode + """
        , """ + mode.upper() + """(meanbp) AS meanbp_""" + mode + """
        , """ + mode.upper() + """(resprate) AS resprate_""" + mode + """
        , """ + mode.upper() + """(tempc) AS tempc_""" + mode + """
        , """ + mode.upper() + """(spo2) AS spo2_""" + mode + """
        , """ + mode.upper() + """(gcseye) AS gcseye_""" + mode + """
        , """ + mode.upper() + """(gcsverbal) AS gcsverbal_""" + mode + """
        , """ + mode.upper() + """(gcsmotor) AS gcsmotor_""" + mode + """
        FROM vitals_stg_3
        GROUP BY stay_id
    )
    SELECT * FROM vitals_stg_4
    """

    vitals = pd.read_sql_query(query, con)

    return vitals


def getInhospitalMortality():
    query = query_schema + \
    """
    SELECT
    icu.stay_id, adm.hospital_expire_flag
    FROM
    mimiciv.icustays icu
    INNER JOIN mimiciv.admissions adm
    ON adm.hadm_id = icu.hadm_id
    """

    mortality = pd.read_sql_query(query, con)

    return mortality


def getFilteredCohort(duration=24):
    query = query_schema + \
    """
    SELECT
    icu.stay_id
    FROM
    mimiciv.icustays icu
    INNER JOIN mimiciv.admissions adm
    ON adm.hadm_id = icu.hadm_id
    INNER JOIN mimiciv.patients pat
    ON pat.subject_id = adm.subject_id
    AND icu.intime = (
        SELECT MAX(icu_max.intime) FROM mimiciv.icustays icu_max WHERE icu_max.hadm_id = icu.hadm_id
    )
    WHERE (icu.outtime >= icu.intime + interval '""" + str(duration) + """ hour')
    AND (FLOOR(DATE_PART('day', adm.admittime - make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age) > 18
    """

    filtered = pd.read_sql_query(query, con)

    return filtered


