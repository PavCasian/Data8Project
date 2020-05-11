from table_builder import *
from sql_table_builder import create_sql_tables
import time

create_sql_tables()
# it takes 286 seconds to load without loading directly the json files
print('Before class instantiatinon: ', time.perf_counter())
academy_competency_inst = AcademyCompetency()
print('Before academy_competency export: ', time.perf_counter())
academy_competency_inst.export_academy_competency()
print('Before spartan import: ', time.perf_counter())
academy_competency_inst.export_spartan()
print('Before strengthweakness export: ', time.perf_counter())
academy_competency_inst.export_strength_weakness_technology()
print('Before assessment export: ', time.perf_counter())
academy_competency_inst.export_assessment()
print('Before candidate export: ', time.perf_counter())
academy_competency_inst.export_candidate()
print('Before course export: ', time.perf_counter())
academy_competency_inst.export_course()
print('Before TalentTeam export: ', time.perf_counter())
TalentTeam().export_talent_team()
print('After talentTeam import: ', time.perf_counter())


# print('Before course import: ', time.perf_counter())
# Course().export()
# print('Before TaletTeam import: ', time.perf_counter())
# TalentTeam().export()
# print('Before candidate import: ', time.perf_counter())
# Candidate().export()
# print('Before assessment import: ', time.perf_counter())
# Assessment().export()
# print('Before strengthweaknesstechnology import: ', time.perf_counter())
# StrengthWeaknessTechnology().export()
# print('Before spartan import: ', time.perf_counter())
# Spartan().export()
# print('Before academyCompetency import: ', time.perf_counter())
# AcademyCompetency().export()
# print('After academyCompetency import: ', time.perf_counter())

