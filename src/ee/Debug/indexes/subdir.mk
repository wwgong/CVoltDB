################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../indexes/IndexStats.cpp \
../indexes/tableindex.cpp \
../indexes/tableindexfactory.cpp 

OBJS += \
./indexes/IndexStats.o \
./indexes/tableindex.o \
./indexes/tableindexfactory.o 

CPP_DEPS += \
./indexes/IndexStats.d \
./indexes/tableindex.d \
./indexes/tableindexfactory.d 


# Each subdirectory must supply rules for building sources it contributes
indexes/%.o: ../indexes/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


