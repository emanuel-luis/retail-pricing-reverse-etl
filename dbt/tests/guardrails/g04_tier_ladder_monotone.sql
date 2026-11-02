-- g04: in every TIER group where the engine changed a member, price rises strictly with the
-- brand tier (rank). Untouched groups are exempt (see ladder_breaks).
with {{ ladder_breaks_inputs() }}
select * from ({{ ladder_breaks('TIER', 'prices', 'members') }})
