package controller;

import dto.ActionDTO;

import java.io.IOException;
import java.io.ObjectOutputStream;

public interface ActionHandler {
    void handleAction(ActionDTO dto, ObjectOutputStream oos) throws IOException;
}
