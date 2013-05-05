################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../logging/JNILogProxy.cpp \
../logging/LogManager.cpp 

OBJS += \
./logging/JNILogProxy.o \
./logging/LogManager.o 

CPP_DEPS += \
./logging/JNILogProxy.d \
./logging/LogManager.d 


# Each subdirectory must supply rules for building sources it contributes
logging/%.o: ../logging/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


