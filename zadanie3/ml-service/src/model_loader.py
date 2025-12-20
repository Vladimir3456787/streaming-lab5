import onnxruntime as ort
import numpy as np
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ONNXModelLoader:
    """Загрузчик ONNX моделей для оптимальной производительности"""
    
    def __init__(self, model_path: str):
        self.model_path = model_path
        
        # Настройки для низкой задержки
        self.session_options = ort.SessionOptions()
        self.session_options.intra_op_num_threads = 1  # Один поток для предсказаний
        self.session_options.inter_op_num_threads = 1
        self.session_options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        self.session_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        
        # Загрузка модели
        self.session = ort.InferenceSession(
            model_path, 
            sess_options=self.session_options,
            providers=['CPUExecutionProvider']  # Можно использовать CUDA если есть GPU
        )
        
        # Получение информации о модели
        self.input_name = self.session.get_inputs()[0].name
        self.output_name = self.session.get_outputs()[0].name
        
        # Загрузка метаданных модели
        self.metadata = self._load_metadata()
        
        logger.info(f"Loaded ONNX model: {model_path}")
        logger.info(f"Input shape: {self.session.get_inputs()[0].shape}")
        logger.info(f"Output shape: {self.session.get_outputs()[0].shape}")
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Загрузка метаданных модели"""
        try:
            metadata_path = self.model_path.replace(".onnx", ".json")
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except:
            return {
                "version": "1.0.0",
                "features": [],
                "created_at": "2024-01-01"
            }
    
    def predict_batch(self, features: np.ndarray) -> np.ndarray:
        """
        Batch prediction с оптимизацией
        """
        # Приведение типов для ONNX
        features = features.astype(np.float32)
        
        # Предсказание
        outputs = self.session.run(
            [self.output_name], 
            {self.input_name: features}
        )
        
        return outputs[0]
    
    def predict_single(self, features: np.ndarray) -> float:
        """
        Оптимизированное одиночное предсказание
        """
        # Добавляем batch dimension если нужно
        if len(features.shape) == 1:
            features = features.reshape(1, -1)
        
        features = features.astype(np.float32)
        outputs = self.session.run(
            [self.output_name], 
            {self.input_name: features}
        )
        
        return float(outputs[0][0])
    
    def get_version(self) -> str:
        """Получение версии модели"""
        return self.metadata.get("version", "1.0.0")
    
    def get_expected_features(self) -> int:
        """Получение количества ожидаемых фич"""
        input_shape = self.session.get_inputs()[0].shape
        return input_shape[1] if len(input_shape) > 1 else input_shape[0]