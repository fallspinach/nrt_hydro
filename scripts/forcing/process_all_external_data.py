import process_nldas2
import process_stage4_archive
import process_stage4_realtime
import process_hrrr_analysis

print('Updating NLDAS2 data archive ...')
process_nldas2.main('')

print('Updating Stage IV non-realtime data archive ...')
process_stage4_archive.main('')

print('Updating Stage IV realtime data archive ...')
process_stage4_realtime.main('')

print('Updating HRRR Analysis data archive ...')
process_hrrr_analysis.main('')

