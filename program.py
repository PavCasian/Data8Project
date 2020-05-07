from table_builder import *
import time

print('Before course import: ', time.perf_counter())
Course().export()
print('Before TaletTeam import: ', time.perf_counter())
TalentTeam().export()
print('Before candidate import: ', time.perf_counter())
Candidate().export()
print('Before assessment import: ', time.perf_counter())
Assessment().export()
print('Before strengthweaknesstechnology import: ', time.perf_counter())
StrengthWeaknessTechnology().export()
print('Before spartan import: ', time.perf_counter())
Spartan().export()
print('Before academyCompetency import: ', time.perf_counter())
AcademyCompetency().export()
print('After academyCompetency import: ', time.perf_counter())

