from table_builder import *
from sql_table_builder import create_sql_tables
import time

# it takes circa 634 seconds when loading directly the json files (take around 5 minutes on their own) - currently set
# it takes circa 286 seconds to load without loading directly the json files

create_sql_tables()
print('Before class instantiation: ', time.perf_counter())
academy_competency_inst = AcademyCompetency()
print('Before academy_competency export: ', time.perf_counter())
academy_competency_inst.export_academy_competency()
print('Before spartan export: ', time.perf_counter())
academy_competency_inst.export_spartan()
print('Before strengthweakness export: ', time.perf_counter())
academy_competency_inst.export_strength_weakness_technology()
print('Before assessment export: ', time.perf_counter())
academy_competency_inst.export_assessment()
print('Before candidate export: ', time.perf_counter())
academy_competency_inst.export_candidate()
print('Before course export: ', time.perf_counter())
academy_competency_inst.export_course()
print('After course export: ', time.perf_counter())


