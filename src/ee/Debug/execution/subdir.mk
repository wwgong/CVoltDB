################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../execution/IPCTopend.cpp \
../execution/JNITopend.cpp \
../execution/VoltDBEngine.cpp 

OBJS += \
./execution/IPCTopend.o \
./execution/JNITopend.o \
./execution/VoltDBEngine.o 

CPP_DEPS += \
./execution/IPCTopend.d \
./execution/JNITopend.d \
./execution/VoltDBEngine.d 


# Each subdirectory must supply rules for building sources it contributes
execution/%.o: ../execution/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


