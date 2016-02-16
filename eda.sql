select * from dfs.`/home/matt/workspace/analytics/rushing4insights/plays.json` limit 1;

select is_pass, count(*) from dfs.`/home/matt/workspace/analytics/rushing4insights/plays.json` group by is_pass;
