-----------------------建pxf外部表------------------------------

DROP external table public.ext_minio_nanshan_json;
CREATE EXTERNAL TABLE public.ext_minio_nanshan_json(
  "PolicyiD" INTEGER,
  "InsurediD" INTEGER,
  "PolicyNumber" INTEGER,
  "PolicyType" INTEGER,
  "EffectiveDate" VARCHAR(10),
  "ExpiryDate" VARCHAR(10),
  "PaymentStatus" VARCHAR(50),
  "CoverageAmount" INTEGER,
  "TotalPremium" INTEGER,
  "InsuranceProducts" INTEGER
)  LOCATION('pxf://nanshan/nanshanlife_file.json?PROFILE=s3:json&SERVER=minio')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';

-----------------------建GP落地表------------------------------

DROP TABLE IF EXISTS public.dw_minio_nanshan_json;
CREATE TABLE public.dw_minio_nanshan_json(
  "PolicyiD" INTEGER,
  "InsurediD" INTEGER,
  "PolicyNumber" INTEGER,
  "PolicyType" INTEGER,
  "EffectiveDate" DATE,
  "ExpiryDate" DATE,
  "PaymentStatus" VARCHAR(50),
  "CoverageAmount" INTEGER,
  "TotalPremium" INTEGER,
  "InsuranceProducts" INTEGER
)WITH(appendoptimized=TRUE)
DISTRIBUTED BY ("InsurediD")
PARTITION BY RANGE ("EffectiveDate")
  (START (date '2000-01-01') INCLUSIVE
    END (date '2030-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 year') );

-------------------------抄寫-----------------------------------------------

insert into public.dw_minio_nanshan_json(
  "PolicyiD",
  "InsurediD",
  "PolicyNumber",
  "PolicyType",
  "EffectiveDate",
  "ExpiryDate",
  "PaymentStatus",
  "CoverageAmount",
  "TotalPremium",
  "InsuranceProducts"
)(select 
  "PolicyiD",
  "InsurediD",
  "PolicyNumber",
  "PolicyType",
  "EffectiveDate"::DATE,
  "ExpiryDate"::DATE,
  "PaymentStatus",
  "CoverageAmount",
  "TotalPremium",
  "InsuranceProducts" 
  from public.ext_minio_nanshan_json);

-----------------------建CSV落地表------------------------------

DROP TABLE IF EXISTS public.dw_nanshan_insureddetails;
CREATE TABLE public.dw_nanshan_insureddetails (
"InsuredID" INTEGER,
"InsuredName" VARCHAR(50),
"Gender" VARCHAR(10),
"DateOfBirth" DATE,
"Occupation" VARCHAR(50),
"Address" VARCHAR(100),
"Phone" VARCHAR(20),
"Email" VARCHAR(50),
"Nationality" VARCHAR(50),
"MaritalStatus" VARCHAR(20)
)WITH(appendoptimized=TRUE)
DISTRIBUTED BY ("InsuredID")
PARTITION BY RANGE ("EffectiveDate")
  (START (date '1960-01-01') INCLUSIVE
    END (date '2030-01-01') EXCLUSIVE
    EVERY (INTERVAL '10 year') );
