from pg_data_etl import Database
import os
from dotenv import load_dotenv
from pathlib import Path
import geopandas as gpd

load_dotenv()

gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))  # path to g drive folder'

def export_shp():
    query = """
        with seg_crash_ids as (
        select cpr.crn, concat(st_rt_no::int, cty_code::int, seg_no::int) as concatid, nlf_id, nlf_cntl_b, nlf_cntl_e, shape 
        from transportation.padot_rms rms
        inner join transportation.crash_pa_roadway cpr 
        on concat(cpr.route::int, cpr.county::int, cpr.segment::int) = concat(rms.st_rt_no::int, rms.cty_code::int, rms.seg_no::int)
        where (concat(st_rt_no, cty_code, seg_no) ~* '[a-z]') is false
        and district_n::int = 6
        order by concatid desc
        ),
    rear_ends as (
        select collision_type, crn from transportation.crash_pennsylvania where collision_type::int = 1),
    angle as (
        select collision_type, crn from transportation.crash_pennsylvania where collision_type::int = 4),
    left_turn as (
        select a.crn, b.unit_num, b.travel_direction, b.veh_movement from transportation.crash_pennsylvania a 
            inner join transportation.crash_pa_vehicle b 
            on a.crn = b.crn
            where b.veh_movement::int = 12)	
    select 
        a.concatid,
        a.nlf_id,
        a.nlf_cntl_b,
        a.nlf_cntl_e,
        a.shape as geom,
        count(b.crn) as total_crashes,
        sum(b.fatal_count) as fatal_count, 
        sum(b.maj_inj_count) as serious_inj,
        sum(b.ped_count) as ped_count,
        sum(b.bicycle_count) as bike_count,
        count(c.collision_type) as rear_end,
        count(d.collision_type) as angle,
        count(e.crn) as left_turns
    from seg_crash_ids a
    inner join transportation.crash_pennsylvania b 
    on a.crn = b.crn
    left join rear_ends c
    on a.crn = c.crn
    left join angle d 
    on a.crn = d.crn
    left join left_turn e 
    on a.crn = e.crn
    group by a.concatid, a.nlf_id, a.nlf_cntl_b, a.nlf_cntl_e, a.shape
    order by a.concatid
        """
    gdf = gis_db.gdf(query) 
    gdf.to_file(data_folder/'crashes_joined_rms_v2.shp')
    return "exported to g drive data folder"

print(export_shp())
