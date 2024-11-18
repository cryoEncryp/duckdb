/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     EQUALS_GREATER = 269,
     INTEGER_DIVISION = 270,
     POWER_OF = 271,
     LAMBDA_ARROW = 272,
     DOUBLE_ARROW = 273,
     LESS_EQUALS = 274,
     GREATER_EQUALS = 275,
     NOT_EQUALS = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANTI = 292,
     ANY = 293,
     ARRAY = 294,
     AS = 295,
     ASC_P = 296,
     ASOF = 297,
     ASSERTION = 298,
     ASSIGNMENT = 299,
     ASYMMETRIC = 300,
     AT = 301,
     ATTACH = 302,
     ATTRIBUTE = 303,
     AUTHORIZATION = 304,
     BACKWARD = 305,
     BEFORE = 306,
     BEGIN_P = 307,
     BETWEEN = 308,
     BIGINT = 309,
     BINARY = 310,
     BIT = 311,
     BOOLEAN_P = 312,
     BOTH = 313,
     BY = 314,
     CACHE = 315,
     CALL_P = 316,
     CALLED = 317,
     CASCADE = 318,
     CASCADED = 319,
     CASE = 320,
     CAST = 321,
     CATALOG_P = 322,
     CENTURIES_P = 323,
     CENTURY_P = 324,
     CHAIN = 325,
     CHAR_P = 326,
     CHARACTER = 327,
     CHARACTERISTICS = 328,
     CHECK_P = 329,
     CHECKPOINT = 330,
     CLASS = 331,
     CLOSE = 332,
     CLUSTER = 333,
     COALESCE = 334,
     COLLATE = 335,
     COLLATION = 336,
     COLUMN = 337,
     COLUMNS = 338,
     COMMENT = 339,
     COMMENTS = 340,
     COMMIT = 341,
     COMMITTED = 342,
     COMPRESSION = 343,
     CONCURRENTLY = 344,
     CONFIGURATION = 345,
     CONFLICT = 346,
     CONNECTION = 347,
     CONSTRAINT = 348,
     CONSTRAINTS = 349,
     CONTENT_P = 350,
     CONTINUE_P = 351,
     CONVERSION_P = 352,
     COPY = 353,
     COST = 354,
     CREATE_P = 355,
     CROSS = 356,
     CSV = 357,
     CUBE = 358,
     CURRENT_P = 359,
     CURSOR = 360,
     CYCLE = 361,
     DATA_P = 362,
     DATABASE = 363,
     DAY_P = 364,
     DAYS_P = 365,
     DEALLOCATE = 366,
     DEC = 367,
     DECADE_P = 368,
     DECADES_P = 369,
     DECIMAL_P = 370,
     DECLARE = 371,
     DEFAULT = 372,
     DEFAULTS = 373,
     DEFERRABLE = 374,
     DEFERRED = 375,
     DEFINER = 376,
     DELETE_P = 377,
     DELIMITER = 378,
     DELIMITERS = 379,
     DEPENDS = 380,
     DESC_P = 381,
     DESCRIBE = 382,
     DETACH = 383,
     DICTIONARY = 384,
     DISABLE_P = 385,
     DISCARD = 386,
     DISTINCT = 387,
     DO = 388,
     DOCUMENT_P = 389,
     DOMAIN_P = 390,
     DOUBLE_P = 391,
     DROP = 392,
     EACH = 393,
     ELSE = 394,
     ENABLE_P = 395,
     ENCODING = 396,
     ENCRYPTED = 397,
     END_P = 398,
     ENUM_P = 399,
     ESCAPE = 400,
     EVENT = 401,
     EXCEPT = 402,
     EXCLUDE = 403,
     EXCLUDING = 404,
     EXCLUSIVE = 405,
     EXECUTE = 406,
     EXISTS = 407,
     EXPLAIN = 408,
     EXPORT_P = 409,
     EXPORT_STATE = 410,
     EXTENSION = 411,
     EXTENSIONS = 412,
     EXTERNAL = 413,
     EXTRACT = 414,
     FALSE_P = 415,
     FAMILY = 416,
     FETCH = 417,
     FILTER = 418,
     FIRST_P = 419,
     FLOAT_P = 420,
     FOLLOWING = 421,
     FOR = 422,
     FORCE = 423,
     FOREIGN = 424,
     FORWARD = 425,
     FREEZE = 426,
     FROM = 427,
     FULL = 428,
     FUNCTION = 429,
     FUNCTIONS = 430,
     GENERATED = 431,
     GLOB = 432,
     GLOBAL = 433,
     GRANT = 434,
     GRANTED = 435,
     GROUP_P = 436,
     GROUPING = 437,
     GROUPING_ID = 438,
     GROUPS = 439,
     HANDLER = 440,
     HAVING = 441,
     HEADER_P = 442,
     HOLD = 443,
     HOUR_P = 444,
     HOURS_P = 445,
     IDENTITY_P = 446,
     IF_P = 447,
     IGNORE_P = 448,
     ILIKE = 449,
     IMMEDIATE = 450,
     IMMUTABLE = 451,
     IMPLICIT_P = 452,
     IMPORT_P = 453,
     IN_P = 454,
     INCLUDE_P = 455,
     INCLUDING = 456,
     INCREMENT = 457,
     INDEX = 458,
     INDEXES = 459,
     INHERIT = 460,
     INHERITS = 461,
     INITIALLY = 462,
     INLINE_P = 463,
     INNER_P = 464,
     INOUT = 465,
     INPUT_P = 466,
     INSENSITIVE = 467,
     INSERT = 468,
     INSTALL = 469,
     INSTEAD = 470,
     INT_P = 471,
     INTEGER = 472,
     INTERSECT = 473,
     INTERVAL = 474,
     INTO = 475,
     INVOKER = 476,
     IS = 477,
     ISNULL = 478,
     ISOLATION = 479,
     JOIN = 480,
     JSON = 481,
     JUMP = 482,
     KEY = 483,
     LABEL = 484,
     LANGUAGE = 485,
     LARGE_P = 486,
     LAST_P = 487,
     LATERAL_P = 488,
     LEADING = 489,
     LEAKPROOF = 490,
     LEFT = 491,
     LEVEL = 492,
     LIKE = 493,
     LIMIT = 494,
     LISTEN = 495,
     LOAD = 496,
     LOCAL = 497,
     LOCATION = 498,
     LOCK_P = 499,
     LOCKED = 500,
     LOGGED = 501,
     MACRO = 502,
     MAP = 503,
     MAPPING = 504,
     MATCH = 505,
     MATERIALIZED = 506,
     MAXVALUE = 507,
     METHOD = 508,
     MICROSECOND_P = 509,
     MICROSECONDS_P = 510,
     MILLENNIA_P = 511,
     MILLENNIUM_P = 512,
     MILLISECOND_P = 513,
     MILLISECONDS_P = 514,
     MINUTE_P = 515,
     MINUTES_P = 516,
     MINVALUE = 517,
     MODE = 518,
     MONTH_P = 519,
     MONTHS_P = 520,
     MOVE = 521,
     NAME_P = 522,
     NAMES = 523,
     NATIONAL = 524,
     NATURAL = 525,
     NCHAR = 526,
     NEW = 527,
     NEXT = 528,
     NO = 529,
     NONE = 530,
     NOT = 531,
     NOTHING = 532,
     NOTIFY = 533,
     NOTNULL = 534,
     NOWAIT = 535,
     NULL_P = 536,
     NULLIF = 537,
     NULLS_P = 538,
     NUMERIC = 539,
     OBJECT_P = 540,
     OF = 541,
     OFF = 542,
     OFFSET = 543,
     OIDS = 544,
     OLD = 545,
     ON = 546,
     ONLY = 547,
     OPERATOR = 548,
     OPTION = 549,
     OPTIONS = 550,
     OR = 551,
     ORDER = 552,
     ORDINALITY = 553,
     OTHERS = 554,
     OUT_P = 555,
     OUTER_P = 556,
     OVER = 557,
     OVERLAPS = 558,
     OVERLAY = 559,
     OVERRIDING = 560,
     OWNED = 561,
     OWNER = 562,
     PARALLEL = 563,
     PARSER = 564,
     PARTIAL = 565,
     PARTITION = 566,
     PASSING = 567,
     PASSWORD = 568,
     PERCENT = 569,
     PERSISTENT = 570,
     PIVOT = 571,
     PIVOT_LONGER = 572,
     PIVOT_WIDER = 573,
     PLACING = 574,
     PLANS = 575,
     POLICY = 576,
     POSITION = 577,
     POSITIONAL = 578,
     PRAGMA_P = 579,
     PRECEDING = 580,
     PRECISION = 581,
     PREPARE = 582,
     PREPARED = 583,
     PRESERVE = 584,
     PRIMARY = 585,
     PRIOR = 586,
     PRIVILEGES = 587,
     PROCEDURAL = 588,
     PROCEDURE = 589,
     PROGRAM = 590,
     PUBLICATION = 591,
     QUALIFY = 592,
     QUARTER_P = 593,
     QUARTERS_P = 594,
     QUOTE = 595,
     RANGE = 596,
     READ_P = 597,
     REAL = 598,
     REASSIGN = 599,
     RECHECK = 600,
     RECURSIVE = 601,
     REF = 602,
     REFERENCES = 603,
     REFERENCING = 604,
     REFRESH = 605,
     REINDEX = 606,
     RELATIVE_P = 607,
     RELEASE = 608,
     RENAME = 609,
     REPEATABLE = 610,
     REPLACE = 611,
     REPLICA = 612,
     RESET = 613,
     RESPECT_P = 614,
     RESTART = 615,
     RESTRICT = 616,
     RETURNING = 617,
     RETURNS = 618,
     REVOKE = 619,
     RIGHT = 620,
     ROLE = 621,
     ROLLBACK = 622,
     ROLLUP = 623,
     ROW = 624,
     ROWS = 625,
     RULE = 626,
     SAMPLE = 627,
     SAVEPOINT = 628,
     SCHEMA = 629,
     SCHEMAS = 630,
     SCOPE = 631,
     SCROLL = 632,
     SEARCH = 633,
     SECOND_P = 634,
     SECONDS_P = 635,
     SECRET = 636,
     SECURITY = 637,
     SELECT = 638,
     SEMI = 639,
     SEQUENCE = 640,
     SEQUENCES = 641,
     SERIALIZABLE = 642,
     SERVER = 643,
     SESSION = 644,
     SET = 645,
     SETOF = 646,
     SETS = 647,
     SHARE = 648,
     SHOW = 649,
     SIMILAR = 650,
     SIMPLE = 651,
     SKIP = 652,
     SMALLINT = 653,
     SNAPSHOT = 654,
     SOME = 655,
     SQL_P = 656,
     STABLE = 657,
     STANDALONE_P = 658,
     START = 659,
     STATEMENT = 660,
     STATISTICS = 661,
     STDIN = 662,
     STDOUT = 663,
     STORAGE = 664,
     STORED = 665,
     STRICT_P = 666,
     STRIP_P = 667,
     STRUCT = 668,
     SUBSCRIPTION = 669,
     SUBSTRING = 670,
     SUMMARIZE = 671,
     SYMMETRIC = 672,
     SYSID = 673,
     SYSTEM_P = 674,
     TABLE = 675,
     TABLES = 676,
     TABLESAMPLE = 677,
     TABLESPACE = 678,
     TEMP = 679,
     TEMPLATE = 680,
     TEMPORARY = 681,
     TEXT_P = 682,
     THEN = 683,
     TIES = 684,
     TIME = 685,
     TIMESTAMP = 686,
     TO = 687,
     TRAILING = 688,
     TRAMPOLINE = 689,
     TRANSACTION = 690,
     TRANSFORM = 691,
     TREAT = 692,
     TRIGGER = 693,
     TRIM = 694,
     TRUE_P = 695,
     TRUNCATE = 696,
     TRUSTED = 697,
     TRY_CAST = 698,
     TYPE_P = 699,
     TYPES_P = 700,
     UNBOUNDED = 701,
     UNCOMMITTED = 702,
     UNENCRYPTED = 703,
     UNION = 704,
     UNIQUE = 705,
     UNKNOWN = 706,
     UNLISTEN = 707,
     UNLOGGED = 708,
     UNPIVOT = 709,
     UNTIL = 710,
     UPDATE = 711,
     USE_P = 712,
     USER = 713,
     USING = 714,
     VACUUM = 715,
     VALID = 716,
     VALIDATE = 717,
     VALIDATOR = 718,
     VALUE_P = 719,
     VALUES = 720,
     VARCHAR = 721,
     VARIADIC = 722,
     VARYING = 723,
     VERBOSE = 724,
     VERSION_P = 725,
     VIEW = 726,
     VIEWS = 727,
     VIRTUAL = 728,
     VOLATILE = 729,
     WEEK_P = 730,
     WEEKS_P = 731,
     WHEN = 732,
     WHERE = 733,
     WHITESPACE_P = 734,
     WINDOW = 735,
     WITH = 736,
     WITHIN = 737,
     WITHOUT = 738,
     WORK = 739,
     WRAPPER = 740,
     WRITE_P = 741,
     XML_P = 742,
     XMLATTRIBUTES = 743,
     XMLCONCAT = 744,
     XMLELEMENT = 745,
     XMLEXISTS = 746,
     XMLFOREST = 747,
     XMLNAMESPACES = 748,
     XMLPARSE = 749,
     XMLPI = 750,
     XMLROOT = 751,
     XMLSERIALIZE = 752,
     XMLTABLE = 753,
     YEAR_P = 754,
     YEARS_P = 755,
     YES_P = 756,
     ZONE = 757,
     NOT_LA = 758,
     NULLS_LA = 759,
     WITH_LA = 760,
     POSTFIXOP = 761,
     UMINUS = 762
   };
#endif
/* Tokens.  */
#define IDENT 258
#define FCONST 259
#define SCONST 260
#define BCONST 261
#define XCONST 262
#define Op 263
#define ICONST 264
#define PARAM 265
#define TYPECAST 266
#define DOT_DOT 267
#define COLON_EQUALS 268
#define EQUALS_GREATER 269
#define INTEGER_DIVISION 270
#define POWER_OF 271
#define LAMBDA_ARROW 272
#define DOUBLE_ARROW 273
#define LESS_EQUALS 274
#define GREATER_EQUALS 275
#define NOT_EQUALS 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANTI 292
#define ANY 293
#define ARRAY 294
#define AS 295
#define ASC_P 296
#define ASOF 297
#define ASSERTION 298
#define ASSIGNMENT 299
#define ASYMMETRIC 300
#define AT 301
#define ATTACH 302
#define ATTRIBUTE 303
#define AUTHORIZATION 304
#define BACKWARD 305
#define BEFORE 306
#define BEGIN_P 307
#define BETWEEN 308
#define BIGINT 309
#define BINARY 310
#define BIT 311
#define BOOLEAN_P 312
#define BOTH 313
#define BY 314
#define CACHE 315
#define CALL_P 316
#define CALLED 317
#define CASCADE 318
#define CASCADED 319
#define CASE 320
#define CAST 321
#define CATALOG_P 322
#define CENTURIES_P 323
#define CENTURY_P 324
#define CHAIN 325
#define CHAR_P 326
#define CHARACTER 327
#define CHARACTERISTICS 328
#define CHECK_P 329
#define CHECKPOINT 330
#define CLASS 331
#define CLOSE 332
#define CLUSTER 333
#define COALESCE 334
#define COLLATE 335
#define COLLATION 336
#define COLUMN 337
#define COLUMNS 338
#define COMMENT 339
#define COMMENTS 340
#define COMMIT 341
#define COMMITTED 342
#define COMPRESSION 343
#define CONCURRENTLY 344
#define CONFIGURATION 345
#define CONFLICT 346
#define CONNECTION 347
#define CONSTRAINT 348
#define CONSTRAINTS 349
#define CONTENT_P 350
#define CONTINUE_P 351
#define CONVERSION_P 352
#define COPY 353
#define COST 354
#define CREATE_P 355
#define CROSS 356
#define CSV 357
#define CUBE 358
#define CURRENT_P 359
#define CURSOR 360
#define CYCLE 361
#define DATA_P 362
#define DATABASE 363
#define DAY_P 364
#define DAYS_P 365
#define DEALLOCATE 366
#define DEC 367
#define DECADE_P 368
#define DECADES_P 369
#define DECIMAL_P 370
#define DECLARE 371
#define DEFAULT 372
#define DEFAULTS 373
#define DEFERRABLE 374
#define DEFERRED 375
#define DEFINER 376
#define DELETE_P 377
#define DELIMITER 378
#define DELIMITERS 379
#define DEPENDS 380
#define DESC_P 381
#define DESCRIBE 382
#define DETACH 383
#define DICTIONARY 384
#define DISABLE_P 385
#define DISCARD 386
#define DISTINCT 387
#define DO 388
#define DOCUMENT_P 389
#define DOMAIN_P 390
#define DOUBLE_P 391
#define DROP 392
#define EACH 393
#define ELSE 394
#define ENABLE_P 395
#define ENCODING 396
#define ENCRYPTED 397
#define END_P 398
#define ENUM_P 399
#define ESCAPE 400
#define EVENT 401
#define EXCEPT 402
#define EXCLUDE 403
#define EXCLUDING 404
#define EXCLUSIVE 405
#define EXECUTE 406
#define EXISTS 407
#define EXPLAIN 408
#define EXPORT_P 409
#define EXPORT_STATE 410
#define EXTENSION 411
#define EXTENSIONS 412
#define EXTERNAL 413
#define EXTRACT 414
#define FALSE_P 415
#define FAMILY 416
#define FETCH 417
#define FILTER 418
#define FIRST_P 419
#define FLOAT_P 420
#define FOLLOWING 421
#define FOR 422
#define FORCE 423
#define FOREIGN 424
#define FORWARD 425
#define FREEZE 426
#define FROM 427
#define FULL 428
#define FUNCTION 429
#define FUNCTIONS 430
#define GENERATED 431
#define GLOB 432
#define GLOBAL 433
#define GRANT 434
#define GRANTED 435
#define GROUP_P 436
#define GROUPING 437
#define GROUPING_ID 438
#define GROUPS 439
#define HANDLER 440
#define HAVING 441
#define HEADER_P 442
#define HOLD 443
#define HOUR_P 444
#define HOURS_P 445
#define IDENTITY_P 446
#define IF_P 447
#define IGNORE_P 448
#define ILIKE 449
#define IMMEDIATE 450
#define IMMUTABLE 451
#define IMPLICIT_P 452
#define IMPORT_P 453
#define IN_P 454
#define INCLUDE_P 455
#define INCLUDING 456
#define INCREMENT 457
#define INDEX 458
#define INDEXES 459
#define INHERIT 460
#define INHERITS 461
#define INITIALLY 462
#define INLINE_P 463
#define INNER_P 464
#define INOUT 465
#define INPUT_P 466
#define INSENSITIVE 467
#define INSERT 468
#define INSTALL 469
#define INSTEAD 470
#define INT_P 471
#define INTEGER 472
#define INTERSECT 473
#define INTERVAL 474
#define INTO 475
#define INVOKER 476
#define IS 477
#define ISNULL 478
#define ISOLATION 479
#define JOIN 480
#define JSON 481
#define JUMP 482
#define KEY 483
#define LABEL 484
#define LANGUAGE 485
#define LARGE_P 486
#define LAST_P 487
#define LATERAL_P 488
#define LEADING 489
#define LEAKPROOF 490
#define LEFT 491
#define LEVEL 492
#define LIKE 493
#define LIMIT 494
#define LISTEN 495
#define LOAD 496
#define LOCAL 497
#define LOCATION 498
#define LOCK_P 499
#define LOCKED 500
#define LOGGED 501
#define MACRO 502
#define MAP 503
#define MAPPING 504
#define MATCH 505
#define MATERIALIZED 506
#define MAXVALUE 507
#define METHOD 508
#define MICROSECOND_P 509
#define MICROSECONDS_P 510
#define MILLENNIA_P 511
#define MILLENNIUM_P 512
#define MILLISECOND_P 513
#define MILLISECONDS_P 514
#define MINUTE_P 515
#define MINUTES_P 516
#define MINVALUE 517
#define MODE 518
#define MONTH_P 519
#define MONTHS_P 520
#define MOVE 521
#define NAME_P 522
#define NAMES 523
#define NATIONAL 524
#define NATURAL 525
#define NCHAR 526
#define NEW 527
#define NEXT 528
#define NO 529
#define NONE 530
#define NOT 531
#define NOTHING 532
#define NOTIFY 533
#define NOTNULL 534
#define NOWAIT 535
#define NULL_P 536
#define NULLIF 537
#define NULLS_P 538
#define NUMERIC 539
#define OBJECT_P 540
#define OF 541
#define OFF 542
#define OFFSET 543
#define OIDS 544
#define OLD 545
#define ON 546
#define ONLY 547
#define OPERATOR 548
#define OPTION 549
#define OPTIONS 550
#define OR 551
#define ORDER 552
#define ORDINALITY 553
#define OTHERS 554
#define OUT_P 555
#define OUTER_P 556
#define OVER 557
#define OVERLAPS 558
#define OVERLAY 559
#define OVERRIDING 560
#define OWNED 561
#define OWNER 562
#define PARALLEL 563
#define PARSER 564
#define PARTIAL 565
#define PARTITION 566
#define PASSING 567
#define PASSWORD 568
#define PERCENT 569
#define PERSISTENT 570
#define PIVOT 571
#define PIVOT_LONGER 572
#define PIVOT_WIDER 573
#define PLACING 574
#define PLANS 575
#define POLICY 576
#define POSITION 577
#define POSITIONAL 578
#define PRAGMA_P 579
#define PRECEDING 580
#define PRECISION 581
#define PREPARE 582
#define PREPARED 583
#define PRESERVE 584
#define PRIMARY 585
#define PRIOR 586
#define PRIVILEGES 587
#define PROCEDURAL 588
#define PROCEDURE 589
#define PROGRAM 590
#define PUBLICATION 591
#define QUALIFY 592
#define QUARTER_P 593
#define QUARTERS_P 594
#define QUOTE 595
#define RANGE 596
#define READ_P 597
#define REAL 598
#define REASSIGN 599
#define RECHECK 600
#define RECURSIVE 601
#define REF 602
#define REFERENCES 603
#define REFERENCING 604
#define REFRESH 605
#define REINDEX 606
#define RELATIVE_P 607
#define RELEASE 608
#define RENAME 609
#define REPEATABLE 610
#define REPLACE 611
#define REPLICA 612
#define RESET 613
#define RESPECT_P 614
#define RESTART 615
#define RESTRICT 616
#define RETURNING 617
#define RETURNS 618
#define REVOKE 619
#define RIGHT 620
#define ROLE 621
#define ROLLBACK 622
#define ROLLUP 623
#define ROW 624
#define ROWS 625
#define RULE 626
#define SAMPLE 627
#define SAVEPOINT 628
#define SCHEMA 629
#define SCHEMAS 630
#define SCOPE 631
#define SCROLL 632
#define SEARCH 633
#define SECOND_P 634
#define SECONDS_P 635
#define SECRET 636
#define SECURITY 637
#define SELECT 638
#define SEMI 639
#define SEQUENCE 640
#define SEQUENCES 641
#define SERIALIZABLE 642
#define SERVER 643
#define SESSION 644
#define SET 645
#define SETOF 646
#define SETS 647
#define SHARE 648
#define SHOW 649
#define SIMILAR 650
#define SIMPLE 651
#define SKIP 652
#define SMALLINT 653
#define SNAPSHOT 654
#define SOME 655
#define SQL_P 656
#define STABLE 657
#define STANDALONE_P 658
#define START 659
#define STATEMENT 660
#define STATISTICS 661
#define STDIN 662
#define STDOUT 663
#define STORAGE 664
#define STORED 665
#define STRICT_P 666
#define STRIP_P 667
#define STRUCT 668
#define SUBSCRIPTION 669
#define SUBSTRING 670
#define SUMMARIZE 671
#define SYMMETRIC 672
#define SYSID 673
#define SYSTEM_P 674
#define TABLE 675
#define TABLES 676
#define TABLESAMPLE 677
#define TABLESPACE 678
#define TEMP 679
#define TEMPLATE 680
#define TEMPORARY 681
#define TEXT_P 682
#define THEN 683
#define TIES 684
#define TIME 685
#define TIMESTAMP 686
#define TO 687
#define TRAILING 688
#define TRAMPOLINE 689
#define TRANSACTION 690
#define TRANSFORM 691
#define TREAT 692
#define TRIGGER 693
#define TRIM 694
#define TRUE_P 695
#define TRUNCATE 696
#define TRUSTED 697
#define TRY_CAST 698
#define TYPE_P 699
#define TYPES_P 700
#define UNBOUNDED 701
#define UNCOMMITTED 702
#define UNENCRYPTED 703
#define UNION 704
#define UNIQUE 705
#define UNKNOWN 706
#define UNLISTEN 707
#define UNLOGGED 708
#define UNPIVOT 709
#define UNTIL 710
#define UPDATE 711
#define USE_P 712
#define USER 713
#define USING 714
#define VACUUM 715
#define VALID 716
#define VALIDATE 717
#define VALIDATOR 718
#define VALUE_P 719
#define VALUES 720
#define VARCHAR 721
#define VARIADIC 722
#define VARYING 723
#define VERBOSE 724
#define VERSION_P 725
#define VIEW 726
#define VIEWS 727
#define VIRTUAL 728
#define VOLATILE 729
#define WEEK_P 730
#define WEEKS_P 731
#define WHEN 732
#define WHERE 733
#define WHITESPACE_P 734
#define WINDOW 735
#define WITH 736
#define WITHIN 737
#define WITHOUT 738
#define WORK 739
#define WRAPPER 740
#define WRITE_P 741
#define XML_P 742
#define XMLATTRIBUTES 743
#define XMLCONCAT 744
#define XMLELEMENT 745
#define XMLEXISTS 746
#define XMLFOREST 747
#define XMLNAMESPACES 748
#define XMLPARSE 749
#define XMLPI 750
#define XMLROOT 751
#define XMLSERIALIZE 752
#define XMLTABLE 753
#define YEAR_P 754
#define YEARS_P 755
#define YES_P 756
#define ZONE 757
#define NOT_LA 758
#define NULLS_LA 759
#define WITH_LA 760
#define POSTFIXOP 761
#define UMINUS 762




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 14 "third_party/libpg_query/grammar/grammar.y"
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGCTEMaterialize			ctematerialize;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGIgnoreNulls          ignorenulls;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
	PGLoadInstallType loadinstalltype;
	PGTransactionStmtType transactiontype;
}
/* Line 1529 of yacc.c.  */
#line 1114 "third_party/libpg_query/grammar/grammar_out.hpp"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


