-- Put here UPDATE statements to the historical database for e.g. renamed/moved tests.
-- If you find an uncommented line in this file, you should remove it - it's from
-- a PR that has already been merged.


-- EXAMPLES

-- Rename a test (without parameter) and/or move it to a different module
-- update test_run set
-- name = 'test_newname', originalname = 'test_newname', path = 'benchmarks/test_newmodule.py'
-- where name == 'test_newname';

-- Retrofit a parameter to a test that didn't have any
-- update test_run set name = 'test_somename[param1-param2]' where name == 'test_somename';


-- ACTUAL CHANGES - please delete everything and write your own

update test_run set name = 'test_basic_sum[fast-thin]' where name == 'test_basic_sum';
