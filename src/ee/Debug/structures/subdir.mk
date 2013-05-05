################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../structures/CompactingPool.cpp \
../structures/ContiguousAllocator.cpp 

OBJS += \
./structures/CompactingPool.o \
./structures/ContiguousAllocator.o 

CPP_DEPS += \
./structures/CompactingPool.d \
./structures/ContiguousAllocator.d 


# Each subdirectory must supply rules for building sources it contributes
structures/%.o: ../structures/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


