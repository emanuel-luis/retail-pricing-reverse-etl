-- g03: in every PACK group where the engine changed a member, unit price (price / pack_units)
-- does not rise with the pack size (rank). Untouched groups are exempt (see ladder_breaks).
with {{ ladder_breaks_inputs() }}
select * from ({{ ladder_breaks('PACK', 'prices', 'members') }})
