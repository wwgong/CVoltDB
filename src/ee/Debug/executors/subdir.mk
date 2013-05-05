################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../executors/abstractexecutor.cpp \
../executors/deleteexecutor.cpp \
../executors/distinctexecutor.cpp \
../executors/executorutil.cpp \
../executors/indexscanexecutor.cpp \
../executors/insertexecutor.cpp \
../executors/limitexecutor.cpp \
../executors/materializeexecutor.cpp \
../executors/nestloopexecutor.cpp \
../executors/nestloopindexexecutor.cpp \
../executors/orderbyexecutor.cpp \
../executors/projectionexecutor.cpp \
../executors/receiveexecutor.cpp \
../executors/sendexecutor.cpp \
../executors/seqscanexecutor.cpp \
../executors/unionexecutor.cpp \
../executors/updateexecutor.cpp 

OBJS += \
./executors/abstractexecutor.o \
./executors/deleteexecutor.o \
./executors/distinctexecutor.o \
./executors/executorutil.o \
./executors/indexscanexecutor.o \
./executors/insertexecutor.o \
./executors/limitexecutor.o \
./executors/materializeexecutor.o \
./executors/nestloopexecutor.o \
./executors/nestloopindexexecutor.o \
./executors/orderbyexecutor.o \
./executors/projectionexecutor.o \
./executors/receiveexecutor.o \
./executors/sendexecutor.o \
./executors/seqscanexecutor.o \
./executors/unionexecutor.o \
./executors/updateexecutor.o 

CPP_DEPS += \
./executors/abstractexecutor.d \
./executors/deleteexecutor.d \
./executors/distinctexecutor.d \
./executors/executorutil.d \
./executors/indexscanexecutor.d \
./executors/insertexecutor.d \
./executors/limitexecutor.d \
./executors/materializeexecutor.d \
./executors/nestloopexecutor.d \
./executors/nestloopindexexecutor.d \
./executors/orderbyexecutor.d \
./executors/projectionexecutor.d \
./executors/receiveexecutor.d \
./executors/sendexecutor.d \
./executors/seqscanexecutor.d \
./executors/unionexecutor.d \
./executors/updateexecutor.d 


# Each subdirectory must supply rules for building sources it contributes
executors/%.o: ../executors/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


