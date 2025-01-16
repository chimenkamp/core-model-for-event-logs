import pickle
from typing import List, Dict

import pandas as pd

from src.types_defintion.event_definition import Event, ProcessEvent, IotEvent, Observation
from src.types_defintion.object_definition import Object, ObjectClassEnum
from src.types_defintion.relationship_definitions import ObjectObjectRelationship, EventObjectRelationship, \
    EventEventRelationship


ROBOT_ARMS = ["R01", "R02", "R03", "R04"]
CONV = ["Conv1", "Conv2", "Conv3", "Conv4"]

COLUMNS: List[str] = ['Q_VFD1_Temperature', 'Q_VFD2_Temperature', 'Q_VFD3_Temperature', 'Q_VFD4_Temperature', 'M_Conv1_Speed_mmps', 'M_Conv2_Speed_mmps', 'M_Conv3_Speed_mmps', 'M_Conv4_Speed_mmps', 'I_R01_Gripper_Pot', 'I_R01_Gripper_Load', 'I_R02_Gripper_Pot', 'I_R02_Gripper_Load', 'I_R03_Gripper_Pot', 'I_R03_Gripper_Load', 'I_R04_Gripper_Pot', 'I_R04_Gripper_Load', 'M_R01_SJointAngle_Degree', 'M_R01_LJointAngle_Degree', 'M_R01_UJointAngle_Degree', 'M_R01_RJointAngle_Degree', 'M_R01_BJointAngle_Degree', 'M_R01_TJointAngle_Degree', 'M_R02_SJointAngle_Degree', 'M_R02_LJointAngle_Degree', 'M_R02_UJointAngle_Degree', 'M_R02_RJointAngle_Degree', 'M_R02_BJointAngle_Degree', 'M_R02_TJointAngle_Degree', 'M_R03_SJointAngle_Degree', 'M_R03_LJointAngle_Degree', 'M_R03_UJointAngle_Degree', 'M_R03_RJointAngle_Degree', 'M_R03_BJointAngle_Degree', 'M_R03_TJointAngle_Degree', 'M_R04_SJointAngle_Degree', 'M_R04_LJointAngle_Degree', 'M_R04_UJointAngle_Degree', 'M_R04_RJointAngle_Degree', 'M_R04_BJointAngle_Degree', 'M_R04_TJointAngle_Degree', 'I_SafetyDoor1_Status', 'I_SafetyDoor2_Status', 'Q_Cell_CycleCount', 'I_MHS_GreenRocketTray', 'timestamp']

# Map the Robot Arm ID to the corresponding columns
ROBOT_ARM_COLUMNS = {
    "R01": ["I_R01_Gripper_Pot", "I_R01_Gripper_Load", "M_R01_SJointAngle_Degree", "M_R01_LJointAngle_Degree", "M_R01_UJointAngle_Degree", "M_R01_RJointAngle_Degree", "M_R01_BJointAngle_Degree", "M_R01_TJointAngle_Degree"],
    "R02": ["I_R02_Gripper_Pot", "I_R02_Gripper_Load", "M_R02_SJointAngle_Degree", "M_R02_LJointAngle_Degree", "M_R02_UJointAngle_Degree", "M_R02_RJointAngle_Degree", "M_R02_BJointAngle_Degree", "M_R02_TJointAngle_Degree"],
    "R03": ["I_R03_Gripper_Pot", "I_R03_Gripper_Load", "M_R03_SJointAngle_Degree", "M_R03_LJointAngle_Degree", "M_R03_UJointAngle_Degree", "M_R03_RJointAngle_Degree", "M_R03_BJointAngle_Degree", "M_R03_TJointAngle_Degree"],
    "R04": ["I_R04_Gripper_Pot", "I_R04_Gripper_Load", "M_R04_SJointAngle_Degree", "M_R04_LJointAngle_Degree", "M_R04_UJointAngle_Degree", "M_R04_RJointAngle_Degree", "M_R04_BJointAngle_Degree", "M_R04_TJointAngle_Degree"]
}

class SensorStreamParser:
    def __init__(self) -> None:
        """
        Initializes the SensorStreamParser class.
        """
        self.objects: List[Object] = []
        self.iot_events: List[IotEvent] = []
        self.process_events: List[ProcessEvent] = []
        self.observations: List[Event] = []
        self.object_object_relationships: List[ObjectObjectRelationship] = []
        self.event_object_relationships: List[EventObjectRelationship] = []
        self.event_event_relationships: List[EventEventRelationship] = []

        for o_id in ROBOT_ARMS:
            robot_arm = Object(
                object_id=f"{o_id}",
                object_class=ObjectClassEnum.MACHINE,
                object_type=f"Robot_Arm_{o_id}",
                attributes={}
            )
            robot_arm_pot = Object(
                object_id=f"{o_id}_Gripper_Pot",
                object_class=ObjectClassEnum.ACTUATOR,
                object_type=f"Robot_Arm_{o_id}_Gripper_Pot",
                attributes={}
            )
            robot_arm_load = Object(
                object_id=f"{o_id}_Gripper_Load",
                object_class=ObjectClassEnum.ACTUATOR,
                object_type=f"Robot_Arm_{o_id}_Gripper_Load",
                attributes={}
            )

            self.objects.extend([robot_arm, robot_arm_pot, robot_arm_load])

            # Create O2o relationships
            self.object_object_relationships.append(
                ObjectObjectRelationship(
                    object_id=robot_arm.object_id,
                    related_object_id=robot_arm_pot.object_id,
                )
            )
            self.object_object_relationships.append(
                ObjectObjectRelationship(
                    object_id=robot_arm.object_id,
                    related_object_id=robot_arm_load.object_id,
                )
            )

        for c in CONV:
            conv = Object(
                object_id=f"Conv_{c}",
                object_class=ObjectClassEnum.MACHINE,
                object_type=f"Conv_{c}",
                attributes={}
            )
            self.objects.append(conv)

    def parse_data(self, data: pd.DataFrame):
        # Iterate over the rows of the DataFrame
        for index, row in data.iterrows():
            # Create an IotEvent object

            for object_ref in self.objects:
                object_id: str = object_ref.object_id

                obj_attributes: List[str] = ROBOT_ARM_COLUMNS[object_id]
                obj_values: List[Dict] = [row[attr] for attr in obj_attributes]
                print(obj_values)


if __name__ == "__main__":
    # load .pkl file
    with open("combined_[1-6].pkl", "rb") as f:
        data: pd.DataFrame = pickle.load(f)

    parser = SensorStreamParser()

    print(list(data.columns))
    # parser.parse_data(data)