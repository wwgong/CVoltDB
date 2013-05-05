################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../common/CompactingStringPool.cpp \
../common/CompactingStringStorage.cpp \
../common/DefaultTupleSerializer.cpp \
../common/NValue.cpp \
../common/RecoveryProtoMessage.cpp \
../common/RecoveryProtoMessageBuilder.cpp \
../common/SQLException.cpp \
../common/SegvException.cpp \
../common/SerializableEEException.cpp \
../common/StringRef.cpp \
../common/ThreadLocalPool.cpp \
../common/TupleSchema.cpp \
../common/UndoLog.cpp \
../common/executorcontext.cpp \
../common/serializeio.cpp \
../common/tabletuple.cpp \
../common/types.cpp 

OBJS += \
./common/CompactingStringPool.o \
./common/CompactingStringStorage.o \
./common/DefaultTupleSerializer.o \
./common/NValue.o \
./common/RecoveryProtoMessage.o \
./common/RecoveryProtoMessageBuilder.o \
./common/SQLException.o \
./common/SegvException.o \
./common/SerializableEEException.o \
./common/StringRef.o \
./common/ThreadLocalPool.o \
./common/TupleSchema.o \
./common/UndoLog.o \
./common/executorcontext.o \
./common/serializeio.o \
./common/tabletuple.o \
./common/types.o 

CPP_DEPS += \
./common/CompactingStringPool.d \
./common/CompactingStringStorage.d \
./common/DefaultTupleSerializer.d \
./common/NValue.d \
./common/RecoveryProtoMessage.d \
./common/RecoveryProtoMessageBuilder.d \
./common/SQLException.d \
./common/SegvException.d \
./common/SerializableEEException.d \
./common/StringRef.d \
./common/ThreadLocalPool.d \
./common/TupleSchema.d \
./common/UndoLog.d \
./common/executorcontext.d \
./common/serializeio.d \
./common/tabletuple.d \
./common/types.d 


# Each subdirectory must supply rules for building sources it contributes
common/%.o: ../common/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


