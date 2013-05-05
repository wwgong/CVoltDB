################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../expressions/abstractexpression.cpp \
../expressions/expressionutil.cpp \
../expressions/tupleaddressexpression.cpp 

OBJS += \
./expressions/abstractexpression.o \
./expressions/expressionutil.o \
./expressions/tupleaddressexpression.o 

CPP_DEPS += \
./expressions/abstractexpression.d \
./expressions/expressionutil.d \
./expressions/tupleaddressexpression.d 


# Each subdirectory must supply rules for building sources it contributes
expressions/%.o: ../expressions/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


