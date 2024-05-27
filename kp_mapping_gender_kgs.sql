WITH end_date AS (
    SELECT uid, org_name, MAX(subb.end_dtz) max_end_dtz
    FROM `fmg-ulm-data-hub-prd.aggregation.person`, UNNEST(sub) subb
    WHERE uid = master_uid AND org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY uid, org_name
),

abo_typ AS (
    SELECT dest.uid, dest.org_name, 
        MIN(CASE WHEN medium = 'print' THEN '1'
                 WHEN medium = 'ep' THEN '2'
                 WHEN medium = 'pc' THEN '3'
                 ELSE '9' END) AS AboTyp
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s
    LEFT JOIN end_date
        ON (dest.uid = end_date.uid AND dest.org_name = end_date.org_name AND s.end_dtz = end_date.max_end_dtz)
    WHERE end_date.max_end_dtz IS NOT NULL AND dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY dest.uid, org_name
),

address AS (
    SELECT uid, org_name, postal_address.residential_unit.street AS Strasse, postal_address.residential_unit.house_number AS Hausnummer,
        postal_address.residential_unit.postcode AS PLZ, postal_address.residential_unit.town AS Stadt, postal_address.residential_unit.country AS Land, 
        postal_address.recipient.first_name AS Vorname, postal_address.recipient.last_name AS Name,
        postal_address.recipient.salutation AS Anrede_GP
    FROM `fmg-ulm-data-hub-prd.aggregation.person`
    WHERE uid = master_uid AND org_name NOT IN ("fmg_mpro", "fmg_pz") AND postal_address.residential_unit.id IS NOT NULL AND postal_address.recipient.company IS NULL
),

KundinSeitMedium AS (
    SELECT dest.uid, dest.org_name, 
        MIN(s.start_dtz) AS Kundin_Seit_Medium
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s
    LEFT JOIN abo_typ
        ON (dest.uid = abo_typ.uid AND dest.org_name = abo_typ.org_name AND s.medium = CASE
                  WHEN abo_typ.AboTyp = '1' THEN 'print'
                  WHEN abo_typ.AboTyp = '2' THEN 'ep'
                  WHEN abo_typ.AboTyp = '3' THEN 'pc'
                  ELSE '9' END)
    WHERE abo_typ.AboTyp IS NOT NULL AND dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY dest.uid, dest.org_name
),

KundinSeitFunke AS (
    SELECT dest.uid, dest.org_name, MIN(s.start_dtz) AS Kundin_Seit_Funke
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s     
    WHERE dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
    GROUP BY dest.uid, dest.org_name
),

main_KuPro AS (
    SELECT dest.uid AS UID, dest.org_name AS Org_Name, MAX(s.title) AS Titel, s.medium AS Medium, MAX(s.role) AS Role,
        CASE WHEN CAST(max_end_dtz AS DATE) > CURRENT_DATE THEN 'aktiv' ELSE 'inaktiv' END AS Status,
        PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Strasse, Hausnummer, Kundin_Seit_Medium, Kundin_Seit_Funke
    FROM `fmg-ulm-data-hub-prd.aggregation.person` dest, UNNEST(sub) s
    LEFT JOIN end_date
        ON (dest.uid = end_date.uid AND dest.org_name = end_date.org_name AND s.end_dtz = end_date.max_end_dtz)
    LEFT JOIN abo_typ
        ON (dest.uid = abo_typ.uid AND dest.org_name = abo_typ.org_name)
    LEFT JOIN address
        ON (dest.uid = address.uid AND dest.org_name = address.org_name)
    LEFT JOIN KundinSeitMedium
        ON (dest.uid = KundinSeitMedium.uid AND dest.org_name = KundinSeitMedium.org_name)
    LEFT JOIN KundinSeitFunke
        ON (dest.uid = KundinSeitFunke.uid AND dest.org_name = KundinSeitFunke.org_name)
    WHERE end_date.max_end_dtz IS NOT NULL AND dest.uid = dest.master_uid AND dest.org_name NOT IN ("fmg_mpro", "fmg_pz")
        AND (CASE WHEN s.medium = 'print' THEN '1'
                  WHEN s.medium = 'ep' THEN '2'
                  WHEN s.medium = 'pc' THEN '3'
                  ELSE '9' END) = abo_typ.AboTyp
    GROUP BY dest.uid, dest.org_name, s.medium, max_end_dtz, Strasse, Hausnummer, PLZ, Stadt, Land, Anrede_GP, Vorname, Name, Kundin_Seit_Medium, Kundin_Seit_Funke
),

GeschlechtAlter AS (
    SELECT 
      bvg.Vorname AS Vorname,
      bvg.Geburtsjahr AS Geburtsjahr,
      EXTRACT(YEAR FROM CURRENT_DATE) - bvg.Geburtsjahr AS Alter,
      bvgg.Geschlecht AS Gender
    FROM `fmg-dcx-data-science.Basistabellen.BAS_Vornamen_Geburtsjahr` bvg
    JOIN `fmg-dcx-data-science.Basistabellen.BAS_Vornamen_Geschlecht` bvgg
    ON LOWER(TRIM(bvg.Vorname)) = LOWER(TRIM(bvgg.Vorname))
    WHERE bvgg.Geschlecht IN ('weiblich', 'männlich')
),
PLZ_to_KGS AS (
    SELECT
        PLZ,
        KGS,
        KGS5,
        KGS3
    FROM `fmg-dcx-data-science.b4p.STA_Postleitzahl_zu_KGS`)

SELECT 
  CAST(kp.UID as STRING) AS UID, 
  kp.Org_Name, 
  kp.Titel, 
  kp.Medium, 
  kp.Role, 
  kp.Status, 
  kp.Anrede_GP, 
  kp.Vorname, 
  kp.Name, 
  kp.Strasse, 
  kp.Hausnummer, 
  kp.PLZ, 
  kp.Stadt, 
  kp.Land,
  kp.Kundin_Seit_Medium, 
  kp.Kundin_Seit_Funke,
  CASE
    WHEN kp.Titel IN ('bhn', 'bsz', 'bz', 'bgz', 'hk', 'bwz', 'bzv', 'bgr', 'bwn') THEN 'Niedersachsen'
    WHEN kp.Titel IN ('ikz', 'wp', 'nrz', 'wr', 'wn', 'waz') THEN 'NRW'
    WHEN kp.Titel IN ('otz', 'tlz', 'ta') THEN 'Thüringen'
    WHEN kp.Titel = 'ha' THEN 'Hamburg'
    WHEN kp.Titel = 'bm' THEN 'Berlin'
    ELSE NULL
  END AS NameTageszeitung,
  CASE
    WHEN kp.Titel IN ('bhn', 'bsz', 'bz', 'bgz', 'hk', 'bwz', 'bzv', 'bgr', 'bwn') THEN 'FUNKE_Medien_NS_Braunschw'
    WHEN kp.Titel IN ('ikz', 'wp', 'nrz', 'wr', 'wn', 'waz') THEN 'FUNKE_Medien_NRW'
    WHEN kp.Titel IN ('otz', 'tlz', 'ta') THEN 'FUNKE_Medien_Thüringen'
    WHEN kp.Titel = 'ha' THEN 'FUNKE_Medien_Hamb_Hamburger_Abendblatt'
    WHEN kp.Titel = 'bm' THEN 'FUNKE_Medien_Berlin_BerlinerMorgenpost'
    ELSE NULL
  END AS NameTageszeitungB4P,
  ga.Gender, 
  ga.Alter,
  plz_kgs.KGS,
  plz_kgs.KGS5,
  plz_kgs.KGS3,
  CASE
    WHEN ga.Alter < 30 THEN 'Unter 30'
    WHEN ga.Alter >= 30 AND ga.Alter < 40 THEN '30-40'
    WHEN ga.Alter >= 40 AND ga.Alter < 50 THEN '40-50'
    WHEN ga.Alter >= 50 AND ga.Alter < 60 THEN '50-60'
    WHEN ga.Alter >= 60 AND ga.Alter < 70 THEN '60-70'
    WHEN ga.Alter >= 70 AND ga.Alter < 80 THEN '70-80'
    WHEN ga.Alter >= 80 THEN 'Über 80'
    ELSE NULL
  END AS Altersklasse_b4p
FROM main_KuPro kp
LEFT JOIN GeschlechtAlter ga
  ON LOWER(TRIM(kp.Vorname)) = LOWER(TRIM(ga.Vorname))
LEFT JOIN PLZ_to_KGS plz_kgs
  ON kp.PLZ = plz_kgs.PLZ
WHERE kp.Titel NOT IN ('kind', 'rs');

## inhaltlich gucken; dass das current date < end date heißt nicht, dass der Kunde aktiv ist; Start Datum muss auch noch < current date!!! case when noch mit rein!
## oben bei max date oben nochmal checken
## schwierigkeit wieder auf gleiches select zu kommen, with beilassen
## beim nächsten mal ohne with, lieber exist
## wenn datum y tage in der zukunft liegt, dann nehm alle, dann nicht nur die die zufällig vom lieferanten höher bewertet wurden
## aus nachhaltigkeitssicht sind not ins nicht zu empfehlen; whitelisting ist immer besser als blacklisting, weil meine sonst neue parameter immer direkt mit reinbekommt, die neu reinkommen; lieber selbst entscheiden was ich haben möchte! könnte passieren, dass man falsche daten selektiert und damit dann auch aussteuert!


############ Tabelle KGS zu PLZ ################

SELECT
*
FROM
  `fmg-dcx-data-science.b4p.STA_Postleitzahl_zu_KGS`
LIMIT
  1000


