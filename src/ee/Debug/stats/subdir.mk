################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../stats/StatsAgent.cpp \
../stats/StatsSource.cpp 

OBJS += \
./stats/StatsAgent.o \
./stats/StatsSource.o 

CPP_DEPS += \
./stats/StatsAgent.d \
./stats/StatsSource.d 


# Each subdirectory must supply rules for building sources it contributes
stats/%.o: ../stats/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


